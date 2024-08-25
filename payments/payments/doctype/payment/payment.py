# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt


import json
from functools import reduce

import frappe
from frappe import ValidationError, _, qb, scrub, throw
from frappe.utils import cint, comma_or, flt, getdate, nowdate

import erpnext
from erpnext.accounts.doctype.bank_account.bank_account import (
	get_bank_account_details,
	get_party_bank_account,
)
from erpnext.accounts.doctype.invoice_discounting.invoice_discounting import (
	get_party_account_based_on_invoice_discounting,
)
from erpnext.accounts.doctype.journal_entry.journal_entry import get_default_bank_cash_account
from erpnext.accounts.doctype.tax_withholding_category.tax_withholding_category import (
	get_party_tax_withholding_details,
)
from erpnext.accounts.general_ledger import make_gl_entries, process_gl_map
from erpnext.accounts.party import get_party_account
from erpnext.accounts.utils import get_account_currency, get_balance_on, get_outstanding_invoices
from erpnext.controllers.accounts_controller import (
	AccountsController,
	get_supplier_block_status,
	validate_taxes_and_charges,
)
from erpnext.setup.utils import get_exchange_rate
from erpnext.accounts.doctype.payment_entry.payment_entry import PaymentEntry

class InvalidPaymentEntry(ValidationError):
	pass


class Payment(PaymentEntry):
    pass


def validate_inclusive_tax(tax, doc):
	def _on_previous_row_error(row_range):
		throw(
			_("To include tax in row {0} in Item rate, taxes in rows {1} must also be included").format(
				tax.idx, row_range
			)
		)

	if cint(getattr(tax, "included_in_paid_amount", None)):
		if tax.charge_type == "Actual":
			# inclusive tax cannot be of type Actual
			throw(
				_("Charge of type 'Actual' in row {0} cannot be included in Item Rate or Paid Amount").format(
					tax.idx
				)
			)
		elif tax.charge_type == "On Previous Row Amount" and not cint(
			doc.get("taxes")[cint(tax.row_id) - 1].included_in_paid_amount
		):
			# referred row should also be inclusive
			_on_previous_row_error(tax.row_id)
		elif tax.charge_type == "On Previous Row Total" and not all(
			[cint(t.included_in_paid_amount for t in doc.get("taxes")[: cint(tax.row_id) - 1])]
		):
			# all rows about the referred tax should be inclusive
			_on_previous_row_error("1 - %d" % (cint(tax.row_id),))
		elif tax.get("category") == "Valuation":
			frappe.throw(_("Valuation type charges can not be marked as Inclusive"))


@frappe.whitelist()
def get_outstanding_reference_documents(args):
	if isinstance(args, str):
		args = json.loads(args)

	if args.get("party_type") == "Member":
		return

	ple = qb.DocType("Payment Ledger Entry")
	common_filter = []
	accounting_dimensions_filter = []
	posting_and_due_date = []

	# confirm that Supplier is not blocked
	if args.get("party_type") == "Supplier":
		supplier_status = get_supplier_block_status(args["party"])
		if supplier_status["on_hold"]:
			if supplier_status["hold_type"] == "All":
				return []
			elif supplier_status["hold_type"] == "Payments":
				if (
					not supplier_status["release_date"] or getdate(nowdate()) <= supplier_status["release_date"]
				):
					return []

	party_account_currency = get_account_currency(args.get("party_account"))
	company_currency = frappe.get_cached_value("Company", args.get("company"), "default_currency")

	# Get positive outstanding sales /purchase invoices
	condition = ""
	if args.get("voucher_type") and args.get("voucher_no"):
		condition = " and voucher_type={0} and voucher_no={1}".format(
			frappe.db.escape(args["voucher_type"]), frappe.db.escape(args["voucher_no"])
		)
		common_filter.append(ple.voucher_type == args["voucher_type"])
		common_filter.append(ple.voucher_no == args["voucher_no"])

	# Add cost center condition
	if args.get("cost_center"):
		condition += " and cost_center='%s'" % args.get("cost_center")
		accounting_dimensions_filter.append(ple.cost_center == args.get("cost_center"))

	date_fields_dict = {
		"posting_date": ["from_posting_date", "to_posting_date"],
		"due_date": ["from_due_date", "to_due_date"],
	}

	for fieldname, date_fields in date_fields_dict.items():
		if args.get(date_fields[0]) and args.get(date_fields[1]):
			condition += " and {0} between '{1}' and '{2}'".format(
				fieldname, args.get(date_fields[0]), args.get(date_fields[1])
			)
			posting_and_due_date.append(ple[fieldname][args.get(date_fields[0]) : args.get(date_fields[1])])

	if args.get("company"):
		condition += " and company = {0}".format(frappe.db.escape(args.get("company")))
		common_filter.append(ple.company == args.get("company"))

	outstanding_invoices = []
	negative_outstanding_invoices = []

	if args.get("get_outstanding_invoices"):
		outstanding_invoices = get_outstanding_invoices(
			args.get("party_type"),
			args.get("party"),
			args.get("party_account"),
			common_filter=common_filter,
			posting_date=posting_and_due_date,
			min_outstanding=args.get("outstanding_amt_greater_than"),
			max_outstanding=args.get("outstanding_amt_less_than"),
			accounting_dimensions=accounting_dimensions_filter,
		)

		outstanding_invoices = split_invoices_based_on_payment_terms(outstanding_invoices)

		for d in outstanding_invoices:
			d["exchange_rate"] = 1
			if party_account_currency != company_currency:
				if d.voucher_type in frappe.get_hooks("invoice_doctypes"):
					d["exchange_rate"] = frappe.db.get_value(d.voucher_type, d.voucher_no, "conversion_rate")
				elif d.voucher_type == "Journal Entry":
					d["exchange_rate"] = get_exchange_rate(
						party_account_currency, company_currency, d.posting_date
					)
			if d.voucher_type in ("Purchase Invoice"):
				d["bill_no"] = frappe.db.get_value(d.voucher_type, d.voucher_no, "bill_no")

		# Get negative outstanding sales /purchase invoices
		if args.get("party_type") != "Employee" and not args.get("voucher_no"):
			negative_outstanding_invoices = get_negative_outstanding_invoices(
				args.get("party_type"),
				args.get("party"),
				args.get("party_account"),
				party_account_currency,
				company_currency,
				condition=condition,
			)

	# Get all SO / PO which are not fully billed or against which full advance not paid
	orders_to_be_billed = []
	if args.get("get_orders_to_be_billed"):
		orders_to_be_billed = get_orders_to_be_billed(
			args.get("posting_date"),
			args.get("party_type"),
			args.get("party"),
			args.get("company"),
			party_account_currency,
			company_currency,
			filters=args,
		)

	data = negative_outstanding_invoices + outstanding_invoices + orders_to_be_billed

	if not data:
		if args.get("get_outstanding_invoices") and args.get("get_orders_to_be_billed"):
			ref_document_type = "invoices or orders"
		elif args.get("get_outstanding_invoices"):
			ref_document_type = "invoices"
		elif args.get("get_orders_to_be_billed"):
			ref_document_type = "orders"

		frappe.msgprint(
			_(
				"No outstanding {0} found for the {1} {2} which qualify the filters you have specified."
			).format(
				ref_document_type, _(args.get("party_type")).lower(), frappe.bold(args.get("party"))
			)
		)

	return data


def split_invoices_based_on_payment_terms(outstanding_invoices):
	invoice_ref_based_on_payment_terms = {}
	for idx, d in enumerate(outstanding_invoices):
		if d.voucher_type in ["Sales Invoice", "Purchase Invoice"]:
			payment_term_template = frappe.db.get_value(
				d.voucher_type, d.voucher_no, "payment_terms_template"
			)
			if payment_term_template:
				allocate_payment_based_on_payment_terms = frappe.db.get_value(
					"Payment Terms Template", payment_term_template, "allocate_payment_based_on_payment_terms"
				)
				if allocate_payment_based_on_payment_terms:
					payment_schedule = frappe.get_all(
						"Payment Schedule", filters={"parent": d.voucher_no}, fields=["*"]
					)

					for payment_term in payment_schedule:
						if payment_term.outstanding > 0.1:
							invoice_ref_based_on_payment_terms.setdefault(idx, [])
							invoice_ref_based_on_payment_terms[idx].append(
								frappe._dict(
									{
										"due_date": d.due_date,
										"currency": d.currency,
										"voucher_no": d.voucher_no,
										"voucher_type": d.voucher_type,
										"posting_date": d.posting_date,
										"invoice_amount": flt(d.invoice_amount),
										"outstanding_amount": flt(d.outstanding_amount),
										"payment_amount": payment_term.payment_amount,
										"payment_term": payment_term.payment_term,
									}
								)
							)

	outstanding_invoices_after_split = []
	if invoice_ref_based_on_payment_terms:
		for idx, ref in invoice_ref_based_on_payment_terms.items():
			voucher_no = ref[0]["voucher_no"]
			voucher_type = ref[0]["voucher_type"]

			frappe.msgprint(
				_("Spliting {} {} into {} row(s) as per Payment Terms").format(
					voucher_type, voucher_no, len(ref)
				),
				alert=True,
			)

			outstanding_invoices_after_split += invoice_ref_based_on_payment_terms[idx]

			existing_row = list(filter(lambda x: x.get("voucher_no") == voucher_no, outstanding_invoices))
			index = outstanding_invoices.index(existing_row[0])
			outstanding_invoices.pop(index)

	outstanding_invoices_after_split += outstanding_invoices
	return outstanding_invoices_after_split


def get_orders_to_be_billed(
	posting_date,
	party_type,
	party,
	company,
	party_account_currency,
	company_currency,
	cost_center=None,
	filters=None,
):
	if party_type == "Customer":
		voucher_type = "Sales Order"
	elif party_type == "Supplier":
		voucher_type = "Purchase Order"
	elif party_type == "Employee":
		voucher_type = None

	# Add cost center condition
	if voucher_type:
		doc = frappe.get_doc({"doctype": voucher_type})
		condition = ""
		if doc and hasattr(doc, "cost_center") and doc.cost_center:
			condition = " and cost_center='%s'" % cost_center

	orders = []
	if voucher_type:
		if party_account_currency == company_currency:
			grand_total_field = "base_grand_total"
			rounded_total_field = "base_rounded_total"
		else:
			grand_total_field = "grand_total"
			rounded_total_field = "rounded_total"

		orders = frappe.db.sql(
			"""
			select
				name as voucher_no,
				if({rounded_total_field}, {rounded_total_field}, {grand_total_field}) as invoice_amount,
				(if({rounded_total_field}, {rounded_total_field}, {grand_total_field}) - advance_paid) as outstanding_amount,
				transaction_date as posting_date
			from
				`tab{voucher_type}`
			where
				{party_type} = %s
				and docstatus = 1
				and company = %s
				and ifnull(status, "") != "Closed"
				and if({rounded_total_field}, {rounded_total_field}, {grand_total_field}) > advance_paid
				and abs(100 - per_billed) > 0.01
				{condition}
			order by
				transaction_date, name
		""".format(
				**{
					"rounded_total_field": rounded_total_field,
					"grand_total_field": grand_total_field,
					"voucher_type": voucher_type,
					"party_type": scrub(party_type),
					"condition": condition,
				}
			),
			(party, company),
			as_dict=True,
		)

	order_list = []
	for d in orders:
		if (
			filters
			and filters.get("outstanding_amt_greater_than")
			and filters.get("outstanding_amt_less_than")
			and not (
				flt(filters.get("outstanding_amt_greater_than"))
				<= flt(d.outstanding_amount)
				<= flt(filters.get("outstanding_amt_less_than"))
			)
		):
			continue

		d["voucher_type"] = voucher_type
		# This assumes that the exchange rate required is the one in the SO
		d["exchange_rate"] = get_exchange_rate(party_account_currency, company_currency, posting_date)
		order_list.append(d)

	return order_list


def get_negative_outstanding_invoices(
	party_type,
	party,
	party_account,
	party_account_currency,
	company_currency,
	cost_center=None,
	condition=None,
):
	voucher_type = "Sales Invoice" if party_type == "Customer" else "Purchase Invoice"
	supplier_condition = ""
	if voucher_type == "Purchase Invoice":
		supplier_condition = "and (release_date is null or release_date <= CURRENT_DATE)"
	if party_account_currency == company_currency:
		grand_total_field = "base_grand_total"
		rounded_total_field = "base_rounded_total"
	else:
		grand_total_field = "grand_total"
		rounded_total_field = "rounded_total"

	return frappe.db.sql(
		"""
		select
			"{voucher_type}" as voucher_type, name as voucher_no,
			if({rounded_total_field}, {rounded_total_field}, {grand_total_field}) as invoice_amount,
			outstanding_amount, posting_date,
			due_date, conversion_rate as exchange_rate
		from
			`tab{voucher_type}`
		where
			{party_type} = %s and {party_account} = %s and docstatus = 1 and
			outstanding_amount < 0
			{supplier_condition}
			{condition}
		order by
			posting_date, name
		""".format(
			**{
				"supplier_condition": supplier_condition,
				"condition": condition,
				"rounded_total_field": rounded_total_field,
				"grand_total_field": grand_total_field,
				"voucher_type": voucher_type,
				"party_type": scrub(party_type),
				"party_account": "debit_to" if party_type == "Customer" else "credit_to",
				"cost_center": cost_center,
			}
		),
		(party, party_account),
		as_dict=True,
	)


@frappe.whitelist()
def get_party_details(company, party_type, party, date, cost_center=None):
	bank_account = ""
	if not frappe.db.exists(party_type, party):
		frappe.throw(_("Invalid {0}: {1}").format(party_type, party))

	party_account = get_party_account(party_type, party, company)

	account_currency = get_account_currency(party_account)
	account_balance = get_balance_on(party_account, date, cost_center=cost_center)
	_party_name = "title" if party_type == "Shareholder" else party_type.lower() + "_name"
	party_name = frappe.db.get_value(party_type, party, _party_name)
	party_balance = get_balance_on(party_type=party_type, party=party, cost_center=cost_center)
	if party_type in ["Customer", "Supplier"]:
		bank_account = get_party_bank_account(party_type, party)

	return {
		"party_account": party_account,
		"party_name": party_name,
		"party_account_currency": account_currency,
		"party_balance": party_balance,
		"account_balance": account_balance,
		"bank_account": bank_account,
	}


@frappe.whitelist()
def get_account_details(account, date, cost_center=None):
	frappe.has_permission("Payment", throw=True)

	# to check if the passed account is accessible under reference doctype Payment Entry
	account_list = frappe.get_list(
		"Account", {"name": account}, reference_doctype="Payment", limit=1
	)

	# There might be some user permissions which will allow account under certain doctypes
	# except for Payment Entry, only in such case we should throw permission error
	if not account_list:
		frappe.throw(_("Account: {0} is not permitted under Payment Entry").format(account))

	account_balance = get_balance_on(
		account, date, cost_center=cost_center, ignore_account_permission=True
	)

	return frappe._dict(
		{
			"account_currency": get_account_currency(account),
			"account_balance": account_balance,
			"account_type": frappe.db.get_value("Account", account, "account_type"),
		}
	)


@frappe.whitelist()
def get_company_defaults(company):
	fields = ["write_off_account", "exchange_gain_loss_account", "cost_center"]
	return frappe.get_cached_value("Company", company, fields, as_dict=1)


def get_outstanding_on_journal_entry(name):
	res = frappe.db.sql(
		"SELECT "
		'CASE WHEN party_type IN ("Customer") '
		"THEN ifnull(sum(debit_in_account_currency - credit_in_account_currency), 0) "
		"ELSE ifnull(sum(credit_in_account_currency - debit_in_account_currency), 0) "
		"END as outstanding_amount "
		"FROM `tabGL Entry` WHERE (voucher_no=%s OR against_voucher=%s) "
		"AND party_type IS NOT NULL "
		'AND party_type != ""',
		(name, name),
		as_dict=1,
	)

	outstanding_amount = res[0].get("outstanding_amount", 0) if res else 0

	return outstanding_amount


@frappe.whitelist()
def get_reference_details(reference_doctype, reference_name, party_account_currency):
	total_amount = outstanding_amount = exchange_rate = None

	ref_doc = frappe.get_doc(reference_doctype, reference_name)
	company_currency = ref_doc.get("company_currency") or erpnext.get_company_currency(
		ref_doc.company
	)

	if reference_doctype == "Dunning":
		total_amount = outstanding_amount = ref_doc.get("dunning_amount")
		exchange_rate = 1

	elif reference_doctype == "Journal Entry" and ref_doc.docstatus == 1:
		total_amount = ref_doc.get("total_amount")
		if ref_doc.multi_currency:
			exchange_rate = get_exchange_rate(
				party_account_currency, company_currency, ref_doc.posting_date
			)
		else:
			exchange_rate = 1
			outstanding_amount = get_outstanding_on_journal_entry(reference_name)

	elif reference_doctype != "Journal Entry":
		if not total_amount:
			if party_account_currency == company_currency:
				# for handling cases that don't have multi-currency (base field)
				total_amount = ref_doc.get("grand_total") or ref_doc.get("base_grand_total")
				exchange_rate = 1
			else:
				total_amount = ref_doc.get("grand_total")
		if not exchange_rate:
			# Get the exchange rate from the original ref doc
			# or get it based on the posting date of the ref doc.
			exchange_rate = ref_doc.get("conversion_rate") or get_exchange_rate(
				party_account_currency, company_currency, ref_doc.posting_date
			)

		if reference_doctype in ("Sales Invoice", "Purchase Invoice"):
			outstanding_amount = ref_doc.get("outstanding_amount")
		else:
			outstanding_amount = flt(total_amount) - flt(ref_doc.get("advance_paid"))

	else:
		# Get the exchange rate based on the posting date of the ref doc.
		exchange_rate = get_exchange_rate(party_account_currency, company_currency, ref_doc.posting_date)

	return frappe._dict(
		{
			"due_date": ref_doc.get("due_date"),
			"total_amount": flt(total_amount),
			"outstanding_amount": flt(outstanding_amount),
			"exchange_rate": flt(exchange_rate),
			"bill_no": ref_doc.get("bill_no"),
		}
	)


@frappe.whitelist()
def get_payment_entry(
	dt,
	dn,
	party_amount=None,
	bank_account=None,
	bank_amount=None,
	party_type=None,
	payment_type=None,
	reference_date=None,
):
	reference_doc = None
	doc = frappe.get_doc(dt, dn)
	over_billing_allowance = frappe.db.get_single_value("Accounts Settings", "over_billing_allowance")
	if dt in ("Sales Order", "Purchase Order") and flt(doc.per_billed, 2) >= (
		100.0 + over_billing_allowance
	):
		frappe.throw(_("Can only make payment against unbilled {0}").format(dt))

	if not party_type:
		party_type = set_party_type(dt)

	party_account = set_party_account(dt, dn, doc, party_type)
	party_account_currency = set_party_account_currency(dt, party_account, doc)

	if not payment_type:
		payment_type = set_payment_type(dt, doc)

	grand_total, outstanding_amount = set_grand_total_and_outstanding_amount(
		party_amount, dt, party_account_currency, doc
	)

	# bank or cash
	bank = get_bank_cash_account(doc, bank_account)

	# if default bank or cash account is not set in company master and party has default company bank account, fetch it
	if party_type in ["Customer", "Supplier"] and not bank:
		party_bank_account = get_party_bank_account(party_type, doc.get(scrub(party_type)))
		if party_bank_account:
			account = frappe.db.get_value("Bank Account", party_bank_account, "account")
			bank = get_bank_cash_account(doc, account)

	paid_amount, received_amount = set_paid_amount_and_received_amount(
		dt, party_account_currency, bank, outstanding_amount, payment_type, bank_amount, doc
	)

	reference_date = getdate(reference_date)
	paid_amount, received_amount, discount_amount, valid_discounts = apply_early_payment_discount(
		paid_amount, received_amount, doc, party_account_currency, reference_date
	)

	pe = frappe.new_doc("Payment")
	pe.payment_type = payment_type
	pe.company = doc.company
	pe.cost_center = doc.get("cost_center")
	pe.posting_date = nowdate()
	pe.reference_date = reference_date
	pe.mode_of_payment = doc.get("mode_of_payment")
	pe.party_type = party_type
	pe.party = doc.get(scrub(party_type))
	pe.contact_person = doc.get("contact_person")
	pe.contact_email = doc.get("contact_email")
	pe.ensure_supplier_is_not_blocked()

	pe.paid_from = party_account if payment_type == "Receive" else bank.account
	pe.paid_to = party_account if payment_type == "Pay" else bank.account
	pe.paid_from_account_currency = (
		party_account_currency if payment_type == "Receive" else bank.account_currency
	)
	pe.paid_to_account_currency = (
		party_account_currency if payment_type == "Pay" else bank.account_currency
	)
	pe.paid_amount = paid_amount
	pe.received_amount = received_amount
	pe.letter_head = doc.get("letter_head")

	if dt in ["Purchase Order", "Sales Order", "Sales Invoice", "Purchase Invoice"]:
		pe.project = doc.get("project") or reduce(
			lambda prev, cur: prev or cur, [x.get("project") for x in doc.get("items")], None
		)  # get first non-empty project from items

	if pe.party_type in ["Customer", "Supplier"]:
		bank_account = get_party_bank_account(pe.party_type, pe.party)
		pe.set("bank_account", bank_account)
		pe.set_bank_account_data()

	# only Purchase Invoice can be blocked individually
	if doc.doctype == "Purchase Invoice" and doc.invoice_is_blocked():
		frappe.msgprint(_("{0} is on hold till {1}").format(doc.name, doc.release_date))
	else:
		if doc.doctype in (
			"Sales Invoice",
			"Purchase Invoice",
			"Purchase Order",
			"Sales Order",
		) and frappe.get_cached_value(
			"Payment Terms Template",
			{"name": doc.payment_terms_template},
			"allocate_payment_based_on_payment_terms",
		):

			for reference in get_reference_as_per_payment_terms(
				doc.payment_schedule, dt, dn, doc, grand_total, outstanding_amount, party_account_currency
			):
				pe.append("references", reference)
		else:
			if dt == "Dunning":
				pe.append(
					"references",
					{
						"reference_doctype": "Sales Invoice",
						"reference_name": doc.get("sales_invoice"),
						"bill_no": doc.get("bill_no"),
						"due_date": doc.get("due_date"),
						"total_amount": doc.get("outstanding_amount"),
						"outstanding_amount": doc.get("outstanding_amount"),
						"allocated_amount": doc.get("outstanding_amount"),
					},
				)
				pe.append(
					"references",
					{
						"reference_doctype": dt,
						"reference_name": dn,
						"bill_no": doc.get("bill_no"),
						"due_date": doc.get("due_date"),
						"total_amount": doc.get("dunning_amount"),
						"outstanding_amount": doc.get("dunning_amount"),
						"allocated_amount": doc.get("dunning_amount"),
					},
				)
			else:
				pe.append(
					"references",
					{
						"reference_doctype": dt,
						"reference_name": dn,
						"bill_no": doc.get("bill_no"),
						"due_date": doc.get("due_date"),
						"total_amount": grand_total,
						"outstanding_amount": outstanding_amount,
						"allocated_amount": outstanding_amount,
					},
				)

	pe.setup_party_account_field()
	pe.set_missing_values()
	pe.set_missing_ref_details()

	update_accounting_dimensions(pe, doc)

	if party_account and bank:
		pe.set_exchange_rate(ref_doc=reference_doc)
		pe.set_amounts()

		if discount_amount:
			base_total_discount_loss = 0
			if frappe.db.get_single_value("Accounts Settings", "book_tax_discount_loss"):
				base_total_discount_loss = split_early_payment_discount_loss(pe, doc, valid_discounts)

			set_pending_discount_loss(
				pe, doc, discount_amount, base_total_discount_loss, party_account_currency
			)

		pe.set_difference_amount()

	return pe


def update_accounting_dimensions(pe, doc):
	"""
	Updates accounting dimensions in Payment Entry based on the accounting dimensions in the reference document
	"""
	from erpnext.accounts.doctype.accounting_dimension.accounting_dimension import (
		get_accounting_dimensions,
	)

	for dimension in get_accounting_dimensions():
		pe.set(dimension, doc.get(dimension))


def get_bank_cash_account(doc, bank_account):
	bank = get_default_bank_cash_account(
		doc.company, "Bank", mode_of_payment=doc.get("mode_of_payment"), account=bank_account
	)

	if not bank:
		bank = get_default_bank_cash_account(
			doc.company, "Cash", mode_of_payment=doc.get("mode_of_payment"), account=bank_account
		)

	return bank


def set_party_type(dt):
	if dt in ("Sales Invoice", "Sales Order", "Dunning"):
		party_type = "Customer"
	elif dt in ("Purchase Invoice", "Purchase Order"):
		party_type = "Supplier"
	return party_type


def set_party_account(dt, dn, doc, party_type):
	if dt == "Sales Invoice":
		party_account = get_party_account_based_on_invoice_discounting(dn) or doc.debit_to
	elif dt == "Purchase Invoice":
		party_account = doc.credit_to
	else:
		party_account = get_party_account(party_type, doc.get(party_type.lower()), doc.company)
	return party_account


def set_party_account_currency(dt, party_account, doc):
	if dt not in ("Sales Invoice", "Purchase Invoice"):
		party_account_currency = get_account_currency(party_account)
	else:
		party_account_currency = doc.get("party_account_currency") or get_account_currency(party_account)
	return party_account_currency


def set_payment_type(dt, doc):
	if (
		dt == "Sales Order" or (dt in ("Sales Invoice", "Dunning") and doc.outstanding_amount > 0)
	) or (dt == "Purchase Invoice" and doc.outstanding_amount < 0):
		payment_type = "Receive"
	else:
		payment_type = "Pay"
	return payment_type


def set_grand_total_and_outstanding_amount(party_amount, dt, party_account_currency, doc):
	grand_total = outstanding_amount = 0
	if party_amount:
		grand_total = outstanding_amount = party_amount
	elif dt in ("Sales Invoice", "Purchase Invoice"):
		if party_account_currency == doc.company_currency:
			grand_total = doc.base_rounded_total or doc.base_grand_total
		else:
			grand_total = doc.rounded_total or doc.grand_total
		outstanding_amount = doc.outstanding_amount
	elif dt == "Dunning":
		grand_total = doc.grand_total
		outstanding_amount = doc.grand_total
	else:
		if party_account_currency == doc.company_currency:
			grand_total = flt(doc.get("base_rounded_total") or doc.get("base_grand_total"))
		else:
			grand_total = flt(doc.get("rounded_total") or doc.get("grand_total"))
		outstanding_amount = doc.get("outstanding_amount") or (grand_total - flt(doc.advance_paid))
	return grand_total, outstanding_amount


def set_paid_amount_and_received_amount(
	dt, party_account_currency, bank, outstanding_amount, payment_type, bank_amount, doc
):
	paid_amount = received_amount = 0
	if party_account_currency == bank.account_currency:
		paid_amount = received_amount = abs(outstanding_amount)
	else:
		company_currency = frappe.get_cached_value("Company", doc.get("company"), "default_currency")
		if payment_type == "Receive":
			paid_amount = abs(outstanding_amount)
			if bank_amount:
				received_amount = bank_amount
			else:
				if company_currency != bank.account_currency:
					received_amount = paid_amount / doc.get("conversion_rate", 1)
				else:
					received_amount = paid_amount * doc.get("conversion_rate", 1)
		else:
			received_amount = abs(outstanding_amount)
			if bank_amount:
				paid_amount = bank_amount
			else:
				if company_currency != bank.account_currency:
					paid_amount = received_amount / doc.get("conversion_rate", 1)
				else:
					# if party account currency and bank currency is different then populate paid amount as well
					paid_amount = received_amount * doc.get("conversion_rate", 1)

	return paid_amount, received_amount


def apply_early_payment_discount(
	paid_amount, received_amount, doc, party_account_currency, reference_date
):
	total_discount = 0
	valid_discounts = []
	eligible_for_payments = ["Sales Order", "Sales Invoice", "Purchase Order", "Purchase Invoice"]
	has_payment_schedule = hasattr(doc, "payment_schedule") and doc.payment_schedule
	is_multi_currency = party_account_currency != doc.company_currency

	if doc.doctype in eligible_for_payments and has_payment_schedule:
		for term in doc.payment_schedule:
			if not term.discounted_amount and term.discount and reference_date <= term.discount_date:

				if term.discount_type == "Percentage":
					grand_total = doc.get("grand_total") if is_multi_currency else doc.get("base_grand_total")
					discount_amount = flt(grand_total) * (term.discount / 100)
				else:
					discount_amount = term.discount

				# if accounting is done in the same currency, paid_amount = received_amount
				conversion_rate = doc.get("conversion_rate", 1) if is_multi_currency else 1
				discount_amount_in_foreign_currency = discount_amount * conversion_rate

				if doc.doctype == "Sales Invoice":
					paid_amount -= discount_amount
					received_amount -= discount_amount_in_foreign_currency
				else:
					received_amount -= discount_amount
					paid_amount -= discount_amount_in_foreign_currency

				valid_discounts.append({"type": term.discount_type, "discount": term.discount})
				total_discount += discount_amount

		if total_discount:
			currency = doc.get("currency") if is_multi_currency else doc.company_currency
			money = frappe.utils.fmt_money(total_discount, currency=currency)
			frappe.msgprint(_("Discount of {} applied as per Payment Term").format(money), alert=1)

	return paid_amount, received_amount, total_discount, valid_discounts


def set_pending_discount_loss(
	pe, doc, discount_amount, base_total_discount_loss, party_account_currency
):
	# If multi-currency, get base discount amount to adjust with base currency deductions/losses
	if party_account_currency != doc.company_currency:
		discount_amount = discount_amount * doc.get("conversion_rate", 1)

	# Avoid considering miniscule losses
	discount_amount = flt(discount_amount - base_total_discount_loss, doc.precision("grand_total"))

	# Set base discount amount (discount loss/pending rounding loss) in deductions
	if discount_amount > 0.0:
		positive_negative = -1 if pe.payment_type == "Pay" else 1

		# If tax loss booking is enabled, pending loss will be rounding loss.
		# Otherwise it will be the total discount loss.
		book_tax_loss = frappe.db.get_single_value("Accounts Settings", "book_tax_discount_loss")
		account_type = "round_off_account" if book_tax_loss else "default_discount_account"

		pe.set_gain_or_loss(
			account_details={
				"account": frappe.get_cached_value("Company", pe.company, account_type),
				"cost_center": pe.cost_center or frappe.get_cached_value("Company", pe.company, "cost_center"),
				"amount": discount_amount * positive_negative,
			}
		)


def split_early_payment_discount_loss(pe, doc, valid_discounts) -> float:
	"""Split early payment discount into Income Loss & Tax Loss."""
	total_discount_percent = get_total_discount_percent(doc, valid_discounts)

	if not total_discount_percent:
		return 0.0

	base_loss_on_income = add_income_discount_loss(pe, doc, total_discount_percent)
	base_loss_on_taxes = add_tax_discount_loss(pe, doc, total_discount_percent)

	# Round off total loss rather than individual losses to reduce rounding error
	return flt(base_loss_on_income + base_loss_on_taxes, doc.precision("grand_total"))


def get_total_discount_percent(doc, valid_discounts) -> float:
	"""Get total percentage and amount discount applied as a percentage."""
	total_discount_percent = (
		sum(
			discount.get("discount") for discount in valid_discounts if discount.get("type") == "Percentage"
		)
		or 0.0
	)

	# Operate in percentages only as it makes the income & tax split easier
	total_discount_amount = (
		sum(discount.get("discount") for discount in valid_discounts if discount.get("type") == "Amount")
		or 0.0
	)

	if total_discount_amount:
		discount_percentage = (total_discount_amount / doc.get("grand_total")) * 100
		total_discount_percent += discount_percentage
		return total_discount_percent

	return total_discount_percent


def add_income_discount_loss(pe, doc, total_discount_percent) -> float:
	"""Add loss on income discount in base currency."""
	precision = doc.precision("total")
	base_loss_on_income = doc.get("base_total") * (total_discount_percent / 100)

	pe.append(
		"deductions",
		{
			"account": frappe.get_cached_value("Company", pe.company, "default_discount_account"),
			"cost_center": pe.cost_center or frappe.get_cached_value("Company", pe.company, "cost_center"),
			"amount": flt(base_loss_on_income, precision),
		},
	)

	return base_loss_on_income  # Return loss without rounding


def add_tax_discount_loss(pe, doc, total_discount_percentage) -> float:
	"""Add loss on tax discount in base currency."""
	tax_discount_loss = {}
	base_total_tax_loss = 0
	precision = doc.precision("tax_amount_after_discount_amount", "taxes")

	# The same account head could be used more than once
	for tax in doc.get("taxes", []):
		base_tax_loss = tax.get("base_tax_amount_after_discount_amount") * (
			total_discount_percentage / 100
		)

		account = tax.get("account_head")
		if not tax_discount_loss.get(account):
			tax_discount_loss[account] = base_tax_loss
		else:
			tax_discount_loss[account] += base_tax_loss

	for account, loss in tax_discount_loss.items():
		base_total_tax_loss += loss
		if loss == 0.0:
			continue

		pe.append(
			"deductions",
			{
				"account": account,
				"cost_center": pe.cost_center or frappe.get_cached_value("Company", pe.company, "cost_center"),
				"amount": flt(loss, precision),
			},
		)

	return base_total_tax_loss  # Return loss without rounding


def get_reference_as_per_payment_terms(
	payment_schedule, dt, dn, doc, grand_total, outstanding_amount, party_account_currency
):
	references = []
	is_multi_currency_acc = (doc.currency != doc.company_currency) and (
		party_account_currency != doc.company_currency
	)

	for payment_term in payment_schedule:
		payment_term_outstanding = flt(
			payment_term.payment_amount - payment_term.paid_amount, payment_term.precision("payment_amount")
		)
		if not is_multi_currency_acc:
			# If accounting is done in company currency for multi-currency transaction
			payment_term_outstanding = flt(
				payment_term_outstanding * doc.get("conversion_rate"), payment_term.precision("payment_amount")
			)

		if payment_term_outstanding:
			references.append(
				{
					"reference_doctype": dt,
					"reference_name": dn,
					"bill_no": doc.get("bill_no"),
					"due_date": doc.get("due_date"),
					"total_amount": grand_total,
					"outstanding_amount": outstanding_amount,
					"payment_term": payment_term.payment_term,
					"allocated_amount": payment_term_outstanding,
				}
			)

	return references


def get_paid_amount(dt, dn, party_type, party, account, due_date):
	if party_type == "Customer":
		dr_or_cr = "credit_in_account_currency - debit_in_account_currency"
	else:
		dr_or_cr = "debit_in_account_currency - credit_in_account_currency"

	paid_amount = frappe.db.sql(
		"""
		select ifnull(sum({dr_or_cr}), 0) as paid_amount
		from `tabGL Entry`
		where against_voucher_type = %s
			and against_voucher = %s
			and party_type = %s
			and party = %s
			and account = %s
			and due_date = %s
			and {dr_or_cr} > 0
	""".format(
			dr_or_cr=dr_or_cr
		),
		(dt, dn, party_type, party, account, due_date),
	)

	return paid_amount[0][0] if paid_amount else 0


@frappe.whitelist()
def get_party_and_account_balance(
	company, date, paid_from=None, paid_to=None, ptype=None, pty=None, cost_center=None
):
	return frappe._dict(
		{
			"party_balance": get_balance_on(party_type=ptype, party=pty, cost_center=cost_center),
			"paid_from_account_balance": get_balance_on(paid_from, date, cost_center=cost_center),
			"paid_to_account_balance": get_balance_on(paid_to, date=date, cost_center=cost_center),
		}
	)


@frappe.whitelist()
def make_payment_order(source_name, target_doc=None):
	from frappe.model.mapper import get_mapped_doc

	def set_missing_values(source, target):
		target.payment_order_type = "Payment"
		target.append(
			"references",
			dict(
				reference_doctype="Payment",
				reference_name=source.name,
				bank_account=source.party_bank_account,
				amount=source.paid_amount,
				account=source.paid_to,
				supplier=source.party,
				mode_of_payment=source.mode_of_payment,
			),
		)

	doclist = get_mapped_doc(
		"Payment",
		source_name,
		{
			"Payment": {
				"doctype": "Payment Order",
				"validation": {"docstatus": ["=", 1]},
			}
		},
		target_doc,
		set_missing_values,
	)

	return doclist
