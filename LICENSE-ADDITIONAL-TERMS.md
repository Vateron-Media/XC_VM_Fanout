# Additional Terms for XC_VM_Fanout

These additional terms supplement the GNU Affero General Public License,
version 3 ("AGPL"), under which XC_VM_Fanout ("the Program") is licensed. They
are authorized by, and apply only to the extent permitted by, **Section 7 of the
AGPL**. If any provision here would exceed what Section 7 allows, it is limited
to what Section 7 allows; in any conflict, the AGPL itself governs.

Copyright (C) 2026 Vateron Media
Program repository: https://github.com/Vateron-Media/XC_VM_Fanout

---

## 1. Additional permission — separate-process use (AGPL §7, first paragraph)

As an additional permission under Section 7 of the AGPL, Vateron Media grants
the following:

Interacting with the Program at arm's length — by running the Program as a
separate operating-system process and communicating with it only through its
command-line interface, its unix or network sockets, its HTTP endpoints, or
files it reads or writes — does not, by that interaction alone, make the
interacting work a "modified version" of, or a work "based on", the Program.
A separate work that only launches, controls, or exchanges data with the
Program through those interfaces may therefore be licensed under terms of your
choice, including proprietary terms.

Because this is an additional *permission*, Section 7 lets any downstream
recipient remove it from copies they convey. Removing it only ever narrows
rights; it never enlarges anyone's obligations.

## 2. Limit of the permission — linking is NOT covered (AGPL §7, first paragraph)

The permission in Section 1 covers arm's-length, separate-process interaction
ONLY. It does not cover linking, importing, embedding, vendoring, or otherwise
combining any Go package of this module into another program's binary (by static
linking, `go` module import, source copying, or any other means). Any such
combination is a work based on the Program and remains governed by the AGPL in
full, with no additional permission.

## 3. Preservation of legal notices — attribution by distributors (AGPL §7(b))

Under Section 7(b) of the AGPL, anyone who conveys the Program or a covered
work — including any project, installer, package, distribution, image, or
service that ships the Program's binary, OR that automatically downloads or
fetches it — must preserve and display the following notice in that work's
documentation and/or its "About"/credits screen or other Appropriate Legal
Notices:

    XC_VM_Fanout — Copyright (C) 2026 Vateron Media —
    https://github.com/Vateron-Media/XC_VM_Fanout — Licensed under AGPL-3.0

Scope note (stated honestly, per the reach of §7(b)): this clause binds those
who convey the Program — ship or auto-download its binary. A separate work that
neither ships nor downloads the binary, and only communicates at runtime with a
copy the user installed independently, is outside Section 7(b) and is not bound
by this clause.

## 4. Marking of modified versions — forks (AGPL §7(b) and §7(c))

Under Sections 7(b) and 7(c) of the AGPL:

(a) Any modified version or fork of the Program must state prominently, in its
    documentation and its Appropriate Legal Notices, that it is based on
    XC_VM_Fanout, with a link to the original:
    https://github.com/Vateron-Media/XC_VM_Fanout

(b) The attribution shown by `xc_fanout -version` (the notice in Section 3) must
    be preserved in modified versions. You may add notices identifying your own
    changes and mark your version as different from the original, but you may not
    remove or obscure the original attribution or misrepresent the Program's
    origin (§7(c)).

The requirements in Sections 3 and 4 are additional *requirements* under Section
7; unlike the permission in Section 1, they may not be removed by downstream
recipients.

## 5. Text of the AGPL

The text of the AGPL in the `LICENSE` file must not be modified. All additional
terms live only in this file and only within what Section 7 of the AGPL permits.
