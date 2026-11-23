package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/victorcondackdev/ledger-go/ledger"
	"github.com/victorcondackdev/ledger-go/money"
)

// Chart of accounts. Customer and merchant balances are liabilities of
// the PSP; its money at the SPI is an asset.
//
//	spi:reserves          asset      the PSP's settlement account at the SPI (mirrors the simulator balance)
//	clearing:cash-in      asset      deposits that arrived through rails not modelled here (TED, boleto, cash)
//	equity:opening        equity     opening balance of the reserves
//	suspense:pix-in       liability  Pix received for a key no customer or merchant owns
//	customer:<id>         liability  customer wallets (no overdraft)
//	merchant:<id>         liability  merchant balances (no overdraft)
const (
	AcctReserves = ledger.AccountID("spi:reserves")
	AcctCashIn   = ledger.AccountID("clearing:cash-in")
	AcctOpening  = ledger.AccountID("equity:opening")
	AcctSuspense = ledger.AccountID("suspense:pix-in")
)

// CustomerAccount is the ledger account of a customer wallet.
func CustomerAccount(id string) ledger.AccountID { return ledger.AccountID("customer:" + id) }

// MerchantAccount is the ledger account of a merchant.
func MerchantAccount(id string) ledger.AccountID { return ledger.AccountID("merchant:" + id) }

func brl(cents int64) money.Amount { return money.New(cents, money.BRL) }

// books wraps the ledger with the postings this PSP makes. Every write
// carries an idempotency key derived from a business identifier (transfer
// ID, EndToEndId, SPI return ID), so replays after a crash or a duplicate
// webhook are no-ops inside the ledger itself.
type books struct{ l *ledger.Ledger }

func (b books) open(ctx context.Context, a ledger.Account) error {
	a.Currency = money.BRL
	if _, err := b.l.OpenAccount(ctx, a); err != nil && !errors.Is(err, ledger.ErrAccountExists) {
		return err
	}
	return nil
}

// openChart opens the system accounts.
func (b books) openChart(ctx context.Context) error {
	for _, a := range []ledger.Account{
		{ID: AcctReserves, Name: "Settlement account at the SPI", Type: ledger.Asset, Code: "1.1.01"},
		{ID: AcctCashIn, Name: "Cash-in clearing", Type: ledger.Asset, Code: "1.1.09"},
		{ID: AcctOpening, Name: "Opening balance", Type: ledger.Equity, Code: "3.1.01"},
		{ID: AcctSuspense, Name: "Unidentified Pix received", Type: ledger.Liability, Code: "2.1.09"},
	} {
		if err := b.open(ctx, a); err != nil {
			return err
		}
	}
	return nil
}

func (b books) openWallet(ctx context.Context, id ledger.AccountID, name, code string) error {
	return b.open(ctx, ledger.Account{ID: id, Name: name, Type: ledger.Liability, Flags: ledger.NoOverdraft, Code: code})
}

func move(debit, credit ledger.AccountID, cents int64) []ledger.Posting {
	return []ledger.Posting{
		{Account: debit, Direction: ledger.Debit, Amount: brl(cents)},
		{Account: credit, Direction: ledger.Credit, Amount: brl(cents)},
	}
}

// opening books the reserves balance read from the SPI at start-up.
func (b books) opening(ctx context.Context, spiAccount string, cents int64) error {
	if cents <= 0 {
		return nil
	}
	_, err := b.l.Post(ctx, ledger.Request{
		IdempotencyKey: "opening:" + spiAccount, Description: "Opening balance of the SPI settlement account",
		Metadata: map[string]string{"spi_account": spiAccount}, Postings: move(AcctReserves, AcctOpening, cents),
	})
	return err
}

// cashIn credits a wallet from the cash-in clearing account.
func (b books) cashIn(ctx context.Context, key string, wallet ledger.AccountID, cents int64) (ledger.Transaction, error) {
	return b.l.Post(ctx, ledger.Request{
		IdempotencyKey: "cash-in:" + key, Description: "Deposit", Postings: move(AcctCashIn, wallet, cents),
	})
}

// holdPayout reserves the amount of an outgoing Pix: the customer's
// available balance drops, nothing is settled yet.
func (b books) holdPayout(ctx context.Context, t *record, wallet ledger.AccountID) (ledger.Transaction, error) {
	return b.l.Hold(ctx, ledger.Request{
		IdempotencyKey: "hold:" + t.ID,
		Description:    "Pix to " + t.Beneficiary.Name,
		Metadata:       map[string]string{"transfer_id": t.ID, "end_to_end_id": t.EndToEndID},
		Postings:       move(wallet, AcctReserves, t.AmountCents),
	})
}

func (b books) postPayout(ctx context.Context, t *record) (ledger.Transaction, error) {
	return b.l.PostPending(ctx, ledger.ResolveRequest{
		ID: t.Ledger.Hold, IdempotencyKey: "post:" + t.ID, Description: "Pix settled (ACSC)",
		Metadata: map[string]string{"end_to_end_id": t.EndToEndID},
	})
}

func (b books) voidPayout(ctx context.Context, t *record, reason string) (ledger.Transaction, error) {
	return b.l.VoidPending(ctx, ledger.ResolveRequest{
		ID: t.Ledger.Hold, IdempotencyKey: "void:" + t.ID, Description: "Pix not settled: " + reason,
		Metadata: map[string]string{"end_to_end_id": t.EndToEndID, "reason": reason},
	})
}

// inbound credits a Pix received into the settlement account.
func (b books) inbound(ctx context.Context, e2e string, to ledger.AccountID, cents int64, payer string) (ledger.Transaction, error) {
	return b.l.Post(ctx, ledger.Request{
		IdempotencyKey: "pix-in:" + e2e, Description: "Pix received from " + payer,
		Metadata: map[string]string{"end_to_end_id": e2e}, Postings: move(AcctReserves, to, cents),
	})
}

// refundPayout books money returned for an outgoing Pix. A full refund of
// an untouched transfer is a ledger reversal (a mirrored transaction linked
// to the original by Ref); anything partial is a compensating posting,
// because ledger-go reverses whole transactions only.
func (b books) refundPayout(ctx context.Context, t *record, wallet ledger.AccountID, ref Refund) (ledger.Transaction, string, error) {
	meta := map[string]string{"end_to_end_id": t.EndToEndID, "return_id": ref.ID, "reason": ref.Reason, "reverses": string(t.Ledger.Post)}
	desc := fmt.Sprintf("Devolução %s of Pix %s", ref.Reason, t.EndToEndID)
	if ref.AmountCents == t.AmountCents && t.RefundedCents == 0 {
		tx, err := b.l.Reverse(ctx, ledger.ResolveRequest{ID: t.Ledger.Post, IdempotencyKey: "refund:" + ref.ID, Description: desc, Metadata: meta})
		return tx, "reversal", err
	}
	tx, err := b.l.Post(ctx, ledger.Request{IdempotencyKey: "refund:" + ref.ID, Description: desc, Metadata: meta, Postings: move(AcctReserves, wallet, ref.AmountCents)})
	return tx, "compensating", err
}

// refundInbound books money this PSP returned for a Pix it had received.
func (b books) refundInbound(ctx context.Context, in *Inbound, refundID string, cents int64) (ledger.Transaction, error) {
	return b.l.Post(ctx, ledger.Request{
		IdempotencyKey: "refund:" + refundID, Description: "Devolução of Pix received " + in.EndToEndID,
		Metadata: map[string]string{"end_to_end_id": in.EndToEndID, "return_id": refundID}, Postings: move(in.CreditedTo, AcctReserves, cents),
	})
}
