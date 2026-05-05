"""Run the PJM DA report. Usage: python run.py [YYYY-MM-DD]"""
import sys
from datetime import date

from backend.reports.pjm_da_report import run

if __name__ == "__main__":
    target = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(target_date=target)
