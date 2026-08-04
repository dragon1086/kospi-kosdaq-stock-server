"""Prove the block handling works before shipping it to PyPI.

Covers the three things that actually mattered on 2026-08-04: recognising the
block page, refusing to hammer it afterwards, and letting go once it clears.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))

import krx_data_client as k

AM = k.KRXAuthManager
checks = []


def check(ok, label, detail=""):
    checks.append((ok, label))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f" — {detail}" if detail else ""))


print("\n=== 1. 차단 페이지 판별 ===")
# The exact page db-server received.
check(
    AM._looks_blocked(
        "에러페이지 - 한국거래소 | Data Marketplace",
        "Service unavailable The service is not smooth due to temporary access instability.",
    ),
    "실제 차단 페이지를 차단으로 인식",
)
check(
    AM._looks_blocked("Error - KRX | Market Data System", "Service unavailable"),
    "영문 403 페이지도 인식",
)
check(
    not AM._looks_blocked("로그인 - KRX | KRX Data Marketplace", "아이디 비밀번호 로그인"),
    "정상 로그인 페이지는 차단이 아님",
)
check(not AM._looks_blocked("", ""), "빈 페이지는 차단으로 단정하지 않음")

print("\n=== 2. 재시도 정책 ===")
check(AM.MAX_LOGIN_RETRIES == 2, "재시도 5회 → 2회", f"={AM.MAX_LOGIN_RETRIES}")
check(
    AM.BLOCK_COOLDOWN == timedelta(hours=24),
    "쿨다운 24시간 (KRX 안내: 탐지일로부터 1일)",
    str(AM.BLOCK_COOLDOWN),
)
check(
    issubclass(k.KRXBlockedError, k.KRXAuthError),
    "KRXBlockedError 는 KRXAuthError 의 하위",
)

print("\n=== 3. 서킷 브레이커 (파일 기반) ===")
tmp = Path("/tmp/krx_block_test")
tmp.mkdir(exist_ok=True)
mgr = AM.__new__(AM)  # 생성자는 자격증명을 요구하므로 우회
mgr.BLOCK_PATH = tmp / ".krx_blocked"
mgr.BLOCK_PATH.unlink(missing_ok=True)

check(mgr._blocked_until() is None, "기록이 없으면 차단 아님")

mgr._mark_blocked("에러페이지 title=... body=Service unavailable")
until = mgr._blocked_until()
check(until is not None, "차단 기록 후 쿨다운 활성")
if until:
    left = (until - datetime.now()).total_seconds() / 60
    check(23 * 60 < left <= 24 * 60, "남은 쿨다운이 24시간 안쪽", f"{left/60:.1f}시간")

check(mgr.BLOCK_PATH.exists(), "기록이 파일로 남음 — 다음 프로세스도 안다")

mgr._clear_blocked()
check(mgr._blocked_until() is None, "해제 후 즉시 통과")
check(not mgr.BLOCK_PATH.exists(), "해제 시 파일 삭제")

print("\n=== 4. 만료된 기록은 스스로 풀림 ===")
import json

mgr.BLOCK_PATH.write_text(
    json.dumps(
        {
            "blocked_at": (datetime.now() - timedelta(hours=25)).isoformat(),
            "reason": "old",
        }
    )
)
check(mgr._blocked_until() is None, "25시간 전 기록은 쿨다운 만료")

mgr.BLOCK_PATH.write_text("이건 JSON 이 아니다")
check(mgr._blocked_until() is None, "깨진 기록은 차단으로 취급하지 않음")
mgr.BLOCK_PATH.unlink(missing_ok=True)

failed = [n for ok, n in checks if not ok]
print(f"\n=== {len(checks) - len(failed)}/{len(checks)} passed ===")
if failed:
    for n in failed:
        print("  FAILED:", n)
    sys.exit(1)
print("ALL PASS")
