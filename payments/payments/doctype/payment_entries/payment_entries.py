import json
from functools import reduce
from Payments import PaymentEntries
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

class PaymentEntry(AccountsController):
    def __init__(self, *args, **kwargs):
        super(PaymentEntry, self).__init__(*args, **kwargs)
        if not self.is_new():
            self.setup_party_account_field()

    def setup_party_account_field(self):
        self.party_account_field = None
        self.party_account = None
        self.party_account_currency = None

        if self.payment_type == "Receive":
            self.party_account_field = "paid_from"
            self.party_account = self.paid_from
            self.party_account_currency = self.paid_from_account_currency

        elif self.payment_type == "Pay":
            self.party_account_field = "paid_to"
            self.party_account = self.paid_to
            self.party_account_currency = self.paid_to_account_currency

    def validate(self):
        self.setup_party_account_field()
        self.set_missing_values()
        self.set_missing_ref_details()
        self.validate_payment_type()
        self.validate_party_details()
        self.set_exchange_rate()
        self.validate_mandatory()
        self.validate_reference_documents()
        self.set_tax_withholding()
        self.set_amounts()
        self.validate_amounts()
        self.apply_taxes()
        self.set_amounts_after_tax()
        self.clear_unallocated_reference_document_rows()
        self.validate_payment_against_negative_invoice()
        self.validate_transaction_reference()
        self.set_title()
        self.set_remarks()
        self.validate_duplicate_entry()
        self.validate_payment_type_with_outstanding()
        self.validate_allocated_amount()
        self.validate_paid_invoices()
        self.ensure_supplier_is_not_blocked()
        self.set_status()

    def on_submit(self):
        if self.difference_amount:
            frappe.throw(_("Difference Amount must be zero"))
        self.make_gl_entries()
        self.update_outstanding_amounts()
        self.update_advance_paid()
        self.update_payment_schedule()
        self.set_status()

    def on_cancel(self):
        self.ignore_linked_doctypes = (
            "GL Entry",
            "Stock Ledger Entry",
            "Payment Ledger Entry",
            "Repost Payment Ledger",
            "Repost Payment Ledger Items",
        )
        self.make_gl_entries(cancel=1)
        self.update_outstanding_amounts()
        self.update_advance_paid()
        self.delink_advance_entry_references()
        self.update_payment_schedule(cancel=1)
        self.set_payment_req_status()
        self.set_status()