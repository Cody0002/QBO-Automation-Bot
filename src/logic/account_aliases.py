"""Client-scoped account-name aliases.

A QBO chart of accounts sometimes grows a second leaf whose name is already in use under a
different parent. The bare leaf name is then ambiguous, and the matchers in
`transformer.find_id_in_map` / `QBOSync.find_id` deliberately refuse to guess between the two
suffix hits rather than book money to the wrong account (see README "Account matching").

When the analysts' sheet convention *does* resolve the ambiguity, record it here instead of
loosening the matcher. Entries are scoped per client family so one workspace's convention
cannot leak into KZO / KZDW / S5 / UMBER.
"""

from __future__ import annotations

import re

# KZP: the chart of accounts holds "Growth" twice -- under "Marketing Income" and under
# "Marketing Expense". The sheet convention (Category / Sub Category is what tells them apart)
# is that the bare name means the income account, and the expense one carries its parent as a
# " - <parent>" suffix:
#     "Growth"                    -> Marketing Income:Growth
#     "Growth - Marketing Expense" -> Marketing Expense:Growth
# Keys are the normalized (whitespace-collapsed, lowercased) sheet value; values are the QBO
# path, which still goes through exact + suffix-path matching afterwards, so a deeper parent
# in QBO ("Income:Marketing Income:Growth") keeps resolving.
KZP_ACCOUNT_ALIASES: dict[str, str] = {
    "growth": "Marketing Income:Growth",
    "growth - marketing income": "Marketing Income:Growth",
    "growth - marketing expense": "Marketing Expense:Growth",
}


def normalize_account_text(name) -> str:
    """Collapse whitespace and drop padding around ':' path separators.

    Analysts write the qualified form as "Marketing Expense: Growth", while QBO's
    FullyQualifiedName has no spaces around the colon.
    """
    text = re.sub(r"\s+", " ", str(name)).strip()
    return re.sub(r"\s*:\s*", ":", text)


def _is_kzp_workspace(client_name) -> bool:
    return "kzp" in str(client_name or "").lower()


def resolve_account_alias_ex(name, client_name: str = "") -> tuple[str, bool]:
    """Normalize an account name and apply `client_name`'s alias table.

    Returns (resolved_name, alias_applied). `alias_applied` marks a name the table
    deliberately disambiguated, so callers that still fall back to fuzzy matching can refuse
    to guess: "Marketing Income:Growth" scores 0.81 against "Marketing Expense:Growth", which
    would hide exactly the mix-up the alias exists to prevent.
    """
    text = normalize_account_text(name)
    if not text:
        return text, False
    if _is_kzp_workspace(client_name):
        alias = KZP_ACCOUNT_ALIASES.get(text.lower())
        if alias is not None:
            return alias, True
    return text, False


def resolve_account_alias(name, client_name: str = "") -> str:
    """Normalize an account name and apply `client_name`'s alias table, if any."""
    return resolve_account_alias_ex(name, client_name)[0]
