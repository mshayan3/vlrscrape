"""
eval_textsql.py — execution-based accuracy harness for the VLR text-to-SQL app.

For each reference question in vlr_test_suite.xlsx ("Test Log" sheet) it:
  1. Runs the REFERENCE SQL  -> "gold" result rows.
  2. Generates SQL from the QUESTION using the app's own SYSTEM_PROMPT + model
     (imported from app.py, so it tests the exact prompt users hit), runs it
     -> "candidate" result rows.
  3. Compares results SEMANTICALLY (values, not SQL text):
        - ranked questions ("highest/most/top/best/rank/order ...")  -> the
          candidate's top row must contain the gold top row's values.
        - other questions -> gold rows must all appear (by value) in candidate
          rows, and row counts must match.
     Each question is run N times (default 3); we report trials-passed so you
     see model variance, not a single flaky pass/fail.

Why execution-based: two different SQL strings can both be correct, so we score
the ANSWER, not the query. Verdicts: PASS / FAIL / REVIEW (borderline -> eyeball).

Usage (run from repo root, no server needed):
    python tests/eval_textsql.py                       # all questions, 3 trials
    python tests/eval_textsql.py --trials 1            # cheap single pass
    python tests/eval_textsql.py --limit 10            # first 10 only
    python tests/eval_textsql.py --category "ECONOMY"  # one category
    python tests/eval_textsql.py --model gpt-4o        # different model
    python tests/eval_textsql.py --dry-run             # NO OpenAI calls: only
                                                       # runs reference SQL, to
                                                       # sanity-check the gold set
Output: tests/eval_results_<timestamp>.xlsx + a console summary by category.
"""
import os
import re
import sys
import time
import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))  # so we can import app.py

SUITE_PATH = REPO / "vlr_test_suite.xlsx"
RANK_WORDS = ("highest", "most", "top", "best", "lowest", "fewest",
              "rank", "ranking", "order by", "leading", "leader", "worst")

GREEN  = PatternFill("solid", fgColor="C6EFCE")
RED    = PatternFill("solid", fgColor="FFC7CE")
YELLOW = PatternFill("solid", fgColor="FFEB9C")
GREY   = PatternFill("solid", fgColor="E0E0E0")
HEADER = PatternFill("solid", fgColor="305496")


# ----------------------------------------------------------------------
# Result normalisation + comparison
# ----------------------------------------------------------------------
def norm_cell(v):
    """Normalise a single cell so 3, 3.0, '3' and ' NRG ' compare cleanly."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        f = float(v)
        return round(f, 2) if f != int(f) else int(f)
    s = str(v).strip()
    try:
        f = float(s)
        return round(f, 2) if f != int(f) else int(f)
    except ValueError:
        return s.lower()


def split_vals(row):
    """Split a row into (string-entities set, numeric-values list)."""
    strs, nums = set(), []
    for c in row:
        if c is None:
            continue
        nc = norm_cell(c)
        if isinstance(nc, (int, float)):
            nums.append(float(nc))
        else:
            strs.add(nc)
    return strs, nums


def num_match(a, b, rel=0.05, abs_tol=0.5):
    """True if a≈b, tolerating rounding and fraction<->percent (x vs x*100)."""
    candidates = [b, b * 100, b / 100] if b != 0 else [b, 0.0]
    for bb in candidates:
        if abs(a - bb) <= max(abs_tol, rel * max(abs(a), abs(bb))):
            return True
    return False


def any_num_match(gn, cnums):
    return any(num_match(gn, cn) for cn in cnums)


def ent_match(a, b):
    """Same entity if one string-set contains the other (candidate may select
    fewer OR more name columns than the reference). Both must be non-empty."""
    return bool(a) and bool(b) and (a.issubset(b) or b.issubset(a))


def is_ranked(question):
    q = question.lower()
    return any(w in q for w in RANK_WORDS)


def compare(gold_rows, cand_rows, ranked):
    """Semantic verdict: 'PASS' | 'FAIL' | 'REVIEW'.

    Entity-centric: the answer to who/which is the NAME; numbers are compared
    with rounding + fraction/percent tolerance. Right entity but wrong number
    -> REVIEW (surfaced, not hidden). Candidate may select fewer columns than
    the reference, so we never require gold's extra columns to be present.
    """
    if not gold_rows and not cand_rows:
        return "PASS"                      # correct empty
    if not gold_rows and cand_rows:
        return "REVIEW"                    # expected empty, got rows
    if gold_rows and not cand_rows:
        return "FAIL"                      # expected an answer, got nothing

    g = [split_vals(r) for r in gold_rows]
    c = [split_vals(r) for r in cand_rows]

    if ranked:
        g0s, g0n = g[0]
        if g0s:                            # "who/which ..." -> entity is the answer
            for cs, cn in c[:2]:           # allow a tie at the very top
                if ent_match(g0s, cs):
                    if not g0n or any(any_num_match(x, cn) for x in g0n):
                        return "PASS"
                    return "REVIEW"        # right entity, off number (e.g. doubled)
            for cs, _ in c:                # entity present, but not at the top
                if ent_match(g0s, cs):
                    return "REVIEW"
            # entity not found at all. Only if GOLD itself shows a tie at the top
            # (>=2 gold rows sharing the top number) and the candidate's top number
            # equals it, treat as a possible alternate tie member -> REVIEW.
            gold_tied = (len(g) >= 2 and g0n and g[1][1]
                         and any(any_num_match(x, g[1][1]) for x in g0n))
            if gold_tied and any(any_num_match(x, c[0][1]) for x in g0n):
                return "REVIEW"
            return "FAIL"
        # pure-numeric ranked (rare): compare the top number
        if g0n and any(any_num_match(g0n[0], cn) for _, cn in c[:2]):
            return "PASS"
        return "FAIL"

    # count-only candidate: GOLD names an entity (e.g. team + count) but the
    # candidate returned just the number (SELECT COUNT(*) with no label). Surface
    # for review rather than FAIL. NOTE: only when gold HAS an entity and candidate
    # does not — a pure-scalar question (gold also entity-less, e.g. "total maps")
    # must fall through to the normal numeric check so an exact match scores PASS.
    gold_has_ents = any(gs for gs, _ in g)
    cand_has_ents = any(cs for cs, _ in c)
    if gold_has_ents and not cand_has_ents:
        gold_nums = [n for _, gn in g for n in gn]
        cand_nums = [n for _, cn in c for n in cn]
        if gold_nums and all(any(num_match(x, y) for y in cand_nums) for x in gold_nums):
            return "REVIEW"

    # unordered: each gold row must map to a candidate row (entity + >=1 metric)
    matched = 0
    ent_only = 0                           # entity lines up but the number is off
    for gs, gn in g:
        full = False
        ent_hit = False
        for cs, cn in c:
            if gs and not ent_match(gs, cs):
                continue
            ent_hit = ent_hit or bool(gs)  # only counts if gold actually had an entity
            if gn and not any(any_num_match(x, cn) for x in gn):
                continue                   # entity matched but no shared metric
            full = True
            break
        if full:
            matched += 1
        elif ent_hit:
            ent_only += 1
    if matched == len(g) and len(c) == len(g):
        return "PASS"
    if matched >= max(1, len(g) * 0.8):
        return "REVIEW"
    if matched + ent_only >= max(1, len(g) * 0.8):
        return "REVIEW"                    # right entities, numbers need a human look
    return "FAIL"


# ----------------------------------------------------------------------
# SQL generation (reuses app.py's prompt + cleaning, exactly)
# ----------------------------------------------------------------------
def make_generator(model):
    import app as appmod
    from openai import OpenAI
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        sys.exit("OPENAI_API_KEY not set (add it to .env). Use --dry-run to skip generation.")
    client = OpenAI(api_key=api_key)
    system_prompt = appmod.SYSTEM_PROMPT

    def generate(question):
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user", "content": question}],
            temperature=0,
        )
        sql = resp.choices[0].message.content.strip().strip("` \n")
        if sql.lower().startswith("sql"):
            sql = sql[3:].strip()
        return sql

    return generate, str(REPO / "db" / "vlr_v3.db")


def run_sql(db_path, sql):
    """Return (rows, error)."""
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.execute(sql)
        rows = [list(r) for r in cur.fetchall()]
        conn.close()
        return rows, None
    except Exception as e:
        return None, str(e)


def preview(rows, n=2):
    if rows is None:
        return ""
    if not rows:
        return "(empty)"
    return " | ".join(", ".join("" if c is None else str(c) for c in r) for r in rows[:n])


# ----------------------------------------------------------------------
# Load the gold question set
# ----------------------------------------------------------------------
def load_questions():
    wb = openpyxl.load_workbook(SUITE_PATH, data_only=True)
    ws = wb["Test Log"]
    rows = list(ws.iter_rows(values_only=True))
    header = [str(h).strip().lower() if h else "" for h in rows[0]]

    def col(name):
        for i, h in enumerate(header):
            if name in h:
                return i
        return None

    ci = {k: col(k) for k in ("#", "category", "question", "status", "reference sql")}
    out = []
    for r in rows[1:]:
        if not r or ci["question"] is None or not r[ci["question"]]:
            continue
        out.append({
            "num": r[ci["#"]] if ci["#"] is not None else len(out) + 1,
            "category": (r[ci["category"]] or "").strip() if ci["category"] is not None else "",
            "question": str(r[ci["question"]]).strip(),
            "status": (str(r[ci["status"]]).strip() if ci["status"] is not None and r[ci["status"]] else ""),
            "ref_sql": (str(r[ci["reference sql"]]).strip() if ci["reference sql"] is not None and r[ci["reference sql"]] else ""),
        })
    return out


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Execution-based accuracy eval for the text-to-SQL app.")
    ap.add_argument("--trials", type=int, default=3, help="generations per question (variance signal)")
    ap.add_argument("--limit", type=int, help="only the first N questions")
    ap.add_argument("--category", help="filter to one category (substring match)")
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--db", default=str(REPO / "db" / "vlr_v3.db"), help="SQLite DB path")
    ap.add_argument("--dry-run", action="store_true", help="run only reference SQL; no OpenAI calls")
    args = ap.parse_args()

    questions = load_questions()
    if args.category:
        questions = [q for q in questions if args.category.lower() in q["category"].lower()]
    if args.limit:
        questions = questions[:args.limit]
    print(f"Loaded {len(questions)} questions"
          + (f" (category={args.category})" if args.category else "")
          + (" [DRY RUN]" if args.dry_run else f"  model={args.model}  trials={args.trials}"))

    db_path = args.db
    generate = None
    if not args.dry_run:
        generate, _ = make_generator(args.model)

    # results sheet
    out_wb = openpyxl.Workbook()
    ws = out_wb.active
    ws.title = "Auto Eval"
    cols = ["#", "Category", "Question", "Expected", "Verdict",
            f"Trials Passed (/{args.trials})", "Gold Rows", "Gold Preview",
            "Cand Rows", "Cand Preview", "Generated SQL (last)", "Error"]
    ws.append(cols)
    for c in range(1, len(cols) + 1):
        cell = ws.cell(1, c)
        cell.fill = HEADER
        cell.font = Font(color="FFFFFF", bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical="top")

    tally = {"PASS": 0, "FAIL": 0, "REVIEW": 0, "GOLD_ERR": 0}
    by_cat = {}

    for q in questions:
        ranked = is_ranked(q["question"])
        gold_rows, gold_err = (None, None)
        if q["ref_sql"]:
            gold_rows, gold_err = run_sql(db_path, q["ref_sql"])
        expected_empty = "empty" in q["status"].lower()

        if gold_err:
            verdict, passed, cand_rows, cand_sql, err = "GOLD_ERR", 0, None, "", f"ref SQL: {gold_err}"
        elif args.dry_run:
            verdict, passed, cand_rows, cand_sql, err = "—", 0, None, "", ""
        else:
            passed, last_sql, last_rows, last_err = 0, "", None, ""
            for _ in range(args.trials):
                try:
                    cand_sql = generate(q["question"])
                except Exception as e:
                    last_err = f"openai: {e}"; last_sql = ""; continue
                cand_rows, cand_err = run_sql(db_path, cand_sql)
                last_sql, last_rows = cand_sql, cand_rows
                if cand_err:
                    last_err = f"gen SQL: {cand_err}"; continue
                if compare(gold_rows or [], cand_rows or [], ranked) == "PASS":
                    passed += 1
                time.sleep(0.2)
            cand_rows, cand_sql, err = last_rows, last_sql, last_err
            # verdict from the last trial's comparison + pass ratio
            base = compare(gold_rows or [], cand_rows or [], ranked) if cand_rows is not None else "FAIL"
            if passed == args.trials:
                verdict = "PASS"
            elif passed == 0:
                verdict = "FAIL" if base == "FAIL" else "REVIEW"
            else:
                verdict = "REVIEW"

        tally[verdict if verdict in tally else "REVIEW"] = tally.get(verdict if verdict in tally else "REVIEW", 0) + (1 if verdict in tally else 0)
        cat = by_cat.setdefault(q["category"], {"PASS": 0, "FAIL": 0, "REVIEW": 0, "GOLD_ERR": 0, "n": 0})
        cat["n"] += 1
        if verdict in cat:
            cat[verdict] += 1

        ws.append([
            q["num"], q["category"], q["question"],
            "empty(correct)" if expected_empty else "rows",
            verdict, passed,
            ("" if gold_rows is None else len(gold_rows)), preview(gold_rows),
            ("" if cand_rows is None else len(cand_rows)), preview(cand_rows),
            cand_sql, err,
        ])
        fill = {"PASS": GREEN, "FAIL": RED, "REVIEW": YELLOW, "GOLD_ERR": GREY}.get(verdict)
        if fill:
            ws.cell(ws.max_row, 5).fill = fill
        print(f"  #{str(q['num']):>3} [{verdict:8}] {q['question'][:60]}")

    # column widths + freeze
    widths = [5, 14, 50, 14, 9, 14, 9, 40, 9, 40, 50, 30]
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[openpyxl.utils.get_column_letter(i)].width = w
    ws.freeze_panes = "A2"

    # summary sheet
    s = out_wb.create_sheet("Summary", 0)
    s.append(["TEXT-TO-SQL ACCURACY — AUTO EVAL"])
    s.append([f"Generated {datetime.now():%Y-%m-%d %H:%M}",
              "DRY RUN" if args.dry_run else f"model={args.model}  trials={args.trials}"])
    s.append([])
    total = len(questions)
    s.append(["OVERALL", "PASS", "REVIEW", "FAIL", "GOLD_ERR", "TOTAL"])
    s.append(["", tally["PASS"], tally["REVIEW"], tally["FAIL"], tally["GOLD_ERR"], total])
    if total and not args.dry_run:
        s.append(["Pass rate", f"{tally['PASS']/total*100:.0f}%"])
    s.append([])
    s.append(["BY CATEGORY", "PASS", "REVIEW", "FAIL", "GOLD_ERR", "N"])
    for cat, d in sorted(by_cat.items()):
        s.append([cat, d["PASS"], d["REVIEW"], d["FAIL"], d["GOLD_ERR"], d["n"]])
    s.column_dimensions["A"].width = 28
    for col in "BCDEF":
        s.column_dimensions[col].width = 11
    s["A1"].font = Font(bold=True, size=14)

    out_path = REPO / "tests" / f"eval_results_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    out_wb.save(out_path)

    print("\n" + "=" * 50)
    print(f"OVERALL: PASS {tally['PASS']}  REVIEW {tally['REVIEW']}  "
          f"FAIL {tally['FAIL']}  GOLD_ERR {tally['GOLD_ERR']}  / {total}")
    if total and not args.dry_run:
        print(f"Pass rate: {tally['PASS']/total*100:.0f}%  "
              f"(+REVIEW counted optimistically: {(tally['PASS']+tally['REVIEW'])/total*100:.0f}%)")
    print(f"Report: {out_path}")


if __name__ == "__main__":
    main()
