"""
KRX Data Client - 통합 주식 데이터 클라이언트

pykrx와 동일한 인터페이스로 KRX Data Marketplace에서 데이터를 조회합니다.
KRX 직접 로그인 또는 카카오 SNS 로그인을 지원하며, 카카오 로그인 시 2차인증은 비활성화되어 있어야 합니다.

아키텍처:
- KRXAuthManager: KRX/카카오 로그인 관리, 세션 유지, 자동 재로그인
- KRXDataClient: 실제 데이터 조회 API

환경변수:
    KRX_LOGIN_METHOD: 로그인 방식 ("krx" 또는 "kakao", 기본값: "krx")
    KRX_ID: KRX 직접 로그인 아이디 (KRX_LOGIN_METHOD="krx" 일 때 필요, 기본값)
    KRX_PW: KRX 직접 로그인 비밀번호 (KRX_LOGIN_METHOD="krx" 일 때 필요, 기본값)
    KAKAO_ID: 카카오 아이디 (KRX_LOGIN_METHOD="kakao" 일 때 필요)
    KAKAO_PW: 카카오 비밀번호 (KRX_LOGIN_METHOD="kakao" 일 때 필요)

사용법:
    from krx_data_client import KRXDataClient

    # KRX 직접 로그인 (환경변수: KRX_LOGIN_METHOD=krx, KRX_ID, KRX_PW)
    client = KRXDataClient()

    # 또는 카카오 로그인 (환경변수: KAKAO_ID, KAKAO_PW)
    client = KRXDataClient(login_method="kakao")

    # pykrx와 동일한 인터페이스
    df = client.get_market_ohlcv("20240101", "20240131", "005930")
    df = client.get_market_fundamental("20240101", "20240131", "005930")
    df = client.get_market_trading_volume("20240101", "20240131", "005930")
"""

import os
import json
import logging
import asyncio
import functools
import time
import fcntl
import random
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable, TypeVar
from dataclasses import dataclass

from holidays.countries import KR

# MCP 서버 등 이미 이벤트 루프가 실행 중인 환경에서 중첩 실행 허용
import nest_asyncio
nest_asyncio.apply()

import requests
import pandas as pd
from pandas import DataFrame

logger = logging.getLogger(__name__)

# 타입 힌트용
T = TypeVar('T')


class KRXAuthError(Exception):
    """인증 관련 에러"""
    pass


class KRX2FARequiredError(KRXAuthError):
    """2차인증이 활성화되어 있음"""
    def __init__(self):
        super().__init__(
            "카카오 2차인증이 활성화되어 있습니다.\n"
            "2차인증을 비활성화하세요:\n"
            "  - 카카오톡 > 설정 > 카카오계정 > 2단계 인증 > 해제\n"
            "  - 또는 https://accounts.kakao.com > 계정보안 > 2단계 인증 > 해제"
        )


class KRXSessionExpiredError(KRXAuthError):
    """세션 만료"""
    pass


class KRXBlockedError(KRXAuthError):
    """KRX가 이 클라이언트의 접근을 차단했다.

    로그인 실패와는 다르게 다뤄야 한다. 자격증명 문제가 아니므로 재시도해도
    풀리지 않고, 오히려 재시도가 차단을 연장시킨다. 실제로 2026-08-04 오후
    배치가 이 상태에서 265회를 두 시간 동안 재시도했고, 그동안 KRX 데이터를
    한 건도 받지 못했다.

    증상은 로그인 페이지 대신 에러 페이지가 오는 것이다. 그 페이지에는 로그인
    iframe이 없으므로, 예전에는 "로그인 iframe을 찾을 수 없습니다"로만 보여
    페이지 구조가 바뀐 것으로 오진하기 쉬웠다.
    """
    pass


class KRXDataError(Exception):
    """데이터 조회 에러"""
    pass


@dataclass
class SessionInfo:
    """세션 정보"""
    cookies: Dict[str, str]
    last_login: datetime
    expires_at: Optional[datetime] = None


def retry_on_session_expired(max_retries: int = 3, delay: float = 1.0):
    """세션 만료 시 재시도하는 데코레이터"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs) -> T:
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(self, *args, **kwargs)
                except KRXSessionExpiredError as e:
                    last_exception = e
                    logger.warning(f"세션 만료 (시도 {attempt + 1}/{max_retries}), 재로그인...")
                    time.sleep(delay * (attempt + 1))  # exponential backoff
                    try:
                        # 세션 파일 삭제 후 재로그인
                        self._auth_manager._cleanup_session_files()
                        self._auth_manager.login(force=True)
                    except Exception as login_error:
                        logger.error(f"재로그인 실패: {login_error}")
                        if attempt == max_retries - 1:
                            raise
                except Exception as e:
                    # 다른 에러는 재시도하지 않음
                    raise
            raise last_exception
        return wrapper
    return decorator


class KRXAuthManager:
    """
    KRX 인증 관리자

    - KRX 직접 로그인 또는 카카오 SNS 로그인 지원
    - 2차인증 상태 체크 및 에러 발생 (카카오 로그인 시)
    - 세션 쿠키 저장/로드
    - 세션 만료 체크 및 자동 갱신
    - 파일 락을 통한 동시 로그인 방지
    """

    COOKIE_PATH = Path.home() / ".krx_session.json"
    LEGACY_COOKIE_PATH = Path.home() / ".krx_cookies.json"  # 기존 쿠키 파일
    LOCK_PATH = Path.home() / ".krx_session.lock"  # 로그인 락 파일
    BLOCK_PATH = Path.home() / ".krx_blocked"  # 차단 감지 시각 기록
    SESSION_TIMEOUT = timedelta(hours=4)  # 세션 타임아웃 (보수적 설정)
    SESSION_REFRESH_THRESHOLD = timedelta(hours=3)  # 이 시간 이후면 선제적 갱신
    VALIDATION_SKIP_THRESHOLD = timedelta(minutes=5)  # 이 시간 내 검증됐으면 재검증 생략

    # Playwright 타임아웃 설정 (운영 안정성을 위해 충분히 길게)
    PAGE_LOAD_TIMEOUT = 60000  # 60초
    LOGIN_WAIT_TIMEOUT = 30000  # 30초
    # 재시도는 "일시적 경쟁"에만 쓸모가 있다. 차단 상태에서는 시도 자체가
    # 비용이고 차단을 연장시키므로 5회에서 2회로 줄인다. 경쟁은 파일 락이
    # 이미 막고 있고, 남은 경쟁이라면 두 번이면 충분하다.
    MAX_LOGIN_RETRIES = 2

    # 차단이 확인되면 이 시간 동안 로그인을 아예 시도하지 않는다.
    #
    # 24시간인 이유는 KRX 안내문이 그렇게 말하기 때문이다: "해당 IP는 탐지일로부터
    # 1일간 접속이 제한되며, 제한기간 경과 후 자동화 수단을 통한 비정상 대량 조회가
    # 다시 탐지되는 경우 접속 제한이 재적용될 수 있습니다."
    #
    # 더 짧게 잡으면 제한이 아직 살아있는 동안 다시 두드리게 되고, 그 두드림이
    # 곧 재탐지 사유가 되어 차단이 갱신된다. 짧은 쿨다운은 회복을 앞당기는 게
    # 아니라 늦춘다.
    BLOCK_COOLDOWN = timedelta(hours=24)

    # 로그인 페이지 대신 이것이 오면 차단이다. KRX는 차단 시 200으로 에러
    # 페이지를 주기도 하므로 상태 코드만으로는 판별할 수 없다.
    BLOCK_MARKERS = (
        "에러페이지",
        "Service unavailable",
        "temporary access instability",
        "일시적인 접속 불안정",
    )
    LOCK_WAIT_TIMEOUT = 120  # 락 대기 타임아웃 (초) - 로그인에 2분 이상 걸릴 수 있음

    def __init__(
        self,
        kakao_id: Optional[str] = None,
        kakao_pw: Optional[str] = None,
        krx_id: Optional[str] = None,
        krx_pw: Optional[str] = None,
        login_method: Optional[str] = None,
        headless: bool = True,
    ):
        # 로그인 방식 결정 (환경변수 > 파라미터, 기본값: krx)
        self.login_method = (login_method or os.environ.get("KRX_LOGIN_METHOD", "krx")).lower()

        # 카카오 로그인 정보
        self.kakao_id = kakao_id or os.environ.get("KAKAO_ID")
        self.kakao_pw = kakao_pw or os.environ.get("KAKAO_PW")

        # KRX 직접 로그인 정보
        self.krx_id = krx_id or os.environ.get("KRX_ID")
        self.krx_pw = krx_pw or os.environ.get("KRX_PW")

        self.headless = headless

        self._session: Optional[requests.Session] = None
        self._session_info: Optional[SessionInfo] = None
        self._last_validated: Optional[datetime] = None  # 마지막 세션 검증 시간 (파일에서 로드)
        self._browser = None
        self._playwright = None

        # 로그인 방식에 따른 필수 정보 검증
        if self.login_method == "krx":
            if not self.krx_id or not self.krx_pw:
                raise KRXAuthError(
                    "KRX 직접 로그인 정보가 필요합니다.\n"
                    "KRX_ID, KRX_PW 환경변수를 설정하세요."
                )
        else:  # kakao (기본값)
            if not self.kakao_id or not self.kakao_pw:
                raise KRXAuthError(
                    "카카오 로그인 정보가 필요합니다.\n"
                    "KAKAO_ID, KAKAO_PW 환경변수를 설정하세요."
                )

    @property
    def is_logged_in(self) -> bool:
        """로그인 상태 확인"""
        if not self._session_info:
            return False

        # 세션 타임아웃 체크
        if datetime.now() - self._session_info.last_login > self.SESSION_TIMEOUT:
            logger.info("세션 타임아웃")
            return False

        return True

    @property
    def session(self) -> requests.Session:
        """requests 세션 반환"""
        if not self._session:
            self._session = requests.Session()
            self._session.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Origin": "http://data.krx.co.kr",
                "Referer": "http://data.krx.co.kr/",
            })
        return self._session

    def _load_session(self) -> bool:
        """저장된 세션 로드"""
        # 새 형식 파일 시도
        if self.COOKIE_PATH.exists():
            try:
                # 파일 수정 시간도 확인 (race condition 대응)
                file_mtime = datetime.fromtimestamp(self.COOKIE_PATH.stat().st_mtime)
                file_is_fresh = datetime.now() - file_mtime < self.VALIDATION_SKIP_THRESHOLD

                data = json.loads(self.COOKIE_PATH.read_text())
                cookies = data.get("cookies", {})
                krx_cookies = data.get("krx_cookies", [])  # domain 포함된 쿠키 정보
                last_login_str = data.get("last_login")
                last_validated_str = data.get("last_validated")  # 마지막 검증 시간

                if cookies and last_login_str:
                    last_login = datetime.fromisoformat(last_login_str)

                    # 타임아웃 체크
                    if datetime.now() - last_login <= self.SESSION_TIMEOUT:
                        # 세션에 쿠키 적용
                        if krx_cookies:
                            # 새 형식: domain, path 포함된 쿠키 사용
                            for cookie in krx_cookies:
                                self.session.cookies.set(
                                    cookie["name"],
                                    cookie["value"],
                                    domain=cookie.get("domain", "data.krx.co.kr"),
                                    path=cookie.get("path", "/")
                                )
                            logger.debug(f"KRX 쿠키 {len(krx_cookies)}개 로드됨")
                        else:
                            # 기존 형식: domain 하드코딩
                            for name, value in cookies.items():
                                self.session.cookies.set(
                                    name, value,
                                    domain="data.krx.co.kr",
                                    path="/"
                                )

                        self._session_info = SessionInfo(
                            cookies=cookies,
                            last_login=last_login
                        )

                        # 마지막 검증 시간 로드 (파일 수정 시간으로 대체 가능)
                        if last_validated_str:
                            self._last_validated = datetime.fromisoformat(last_validated_str)
                        elif file_is_fresh:
                            # last_validated 없어도 파일이 최근 수정됐으면 신뢰
                            self._last_validated = file_mtime
                            logger.info(f"파일 수정 시간으로 세션 신뢰: {file_mtime}")

                        logger.info("저장된 세션을 로드했습니다.")
                        return True
                    else:
                        logger.info("저장된 세션이 만료되었습니다.")
            except Exception as e:
                logger.warning(f"세션 로드 실패: {e}")

        # 기존 형식 파일 (krx_crawler_client 호환)
        if self.LEGACY_COOKIE_PATH.exists():
            try:
                cookies_list = json.loads(self.LEGACY_COOKIE_PATH.read_text())
                if isinstance(cookies_list, list) and cookies_list:
                    cookies = {c["name"]: c["value"] for c in cookies_list}

                    # 세션에 쿠키 적용 (domain 필수!)
                    for name, value in cookies.items():
                        self.session.cookies.set(
                            name, value,
                            domain="data.krx.co.kr",
                            path="/"
                        )

                    self._session_info = SessionInfo(
                        cookies=cookies,
                        last_login=datetime.now() - timedelta(hours=1)  # 1시간 전으로 설정
                    )

                    logger.info("기존 쿠키 파일을 로드했습니다.")
                    return True
            except Exception as e:
                logger.warning(f"기존 쿠키 로드 실패: {e}")

        return False

    def _save_session(self, cookies: Dict[str, str], update_validated: bool = True, krx_cookies: List[Dict] = None):
        """세션 저장"""
        try:
            now = datetime.now()
            data = {
                "cookies": cookies,
                "krx_cookies": krx_cookies or [],  # domain, path 포함된 전체 쿠키 정보
                "last_login": now.isoformat(),
                "last_validated": now.isoformat() if update_validated else None
            }
            self.COOKIE_PATH.write_text(json.dumps(data, indent=2))
            if update_validated:
                self._last_validated = now
            logger.info("세션을 저장했습니다.")
        except Exception as e:
            logger.warning(f"세션 저장 실패: {e}")

    def _update_last_validated(self):
        """세션 파일의 last_validated만 업데이트 (검증 성공 시 호출)"""
        try:
            if not self.COOKIE_PATH.exists():
                return

            data = json.loads(self.COOKIE_PATH.read_text())
            now = datetime.now()
            data["last_validated"] = now.isoformat()
            self.COOKIE_PATH.write_text(json.dumps(data, indent=2))
            self._last_validated = now
            logger.debug("세션 검증 시간 업데이트")
        except Exception as e:
            logger.warning(f"세션 검증 시간 업데이트 실패: {e}")

    def _cleanup_session_files(self):
        """세션 파일 삭제 (손상/만료 시 호출)"""
        for path in [self.COOKIE_PATH, self.LEGACY_COOKIE_PATH]:
            try:
                if path.exists():
                    path.unlink()
                    logger.info(f"세션 파일 삭제: {path}")
            except Exception as e:
                logger.warning(f"세션 파일 삭제 실패: {path}, {e}")

        # 세션 정보 초기화
        self._session_info = None
        if self._session:
            self._session.cookies.clear()

    @classmethod
    def _looks_blocked(cls, title: str, body: str) -> bool:
        """로그인 페이지가 아니라 차단 페이지인가.

        상태 코드로는 못 가른다. KRX는 차단 시 403을 주기도 하고 200에 에러
        페이지 본문을 실어 주기도 한다. 그래서 내용으로 판별한다.
        """
        haystack = f"{title}\n{body}"
        return any(marker in haystack for marker in cls.BLOCK_MARKERS)

    def _mark_blocked(self, reason: str):
        """차단 시각을 남겨 다음 프로세스도 알 수 있게 한다.

        배치는 프로세스를 여러 번 띄우므로 메모리에만 두면 다음 프로세스가
        다시 차단을 두드린다. 세션 파일과 같은 위치에 파일로 남긴다.
        """
        try:
            self.BLOCK_PATH.write_text(
                json.dumps(
                    {"blocked_at": datetime.now().isoformat(), "reason": reason[:500]},
                    ensure_ascii=False,
                )
            )
            logger.error(
                f"KRX 접근 차단 감지 — {self.BLOCK_COOLDOWN.total_seconds() / 60:.0f}분간 "
                f"로그인을 시도하지 않는다: {reason[:200]}"
            )
        except Exception as e:
            logger.warning(f"차단 기록 실패(무시): {e}")

    def _blocked_until(self) -> Optional[datetime]:
        """쿨다운이 남아 있으면 해제 시각, 아니면 None."""
        try:
            if not self.BLOCK_PATH.exists():
                return None
            blocked_at = datetime.fromisoformat(
                json.loads(self.BLOCK_PATH.read_text())["blocked_at"]
            )
        except Exception:
            # 읽을 수 없는 기록은 없는 것으로 본다 — 차단이 아닌데 막는 쪽이
            # 차단인데 두드리는 쪽보다 낫다고 할 수 없다.
            return None
        until = blocked_at + self.BLOCK_COOLDOWN
        return until if datetime.now() < until else None

    def _clear_blocked(self):
        try:
            if self.BLOCK_PATH.exists():
                self.BLOCK_PATH.unlink()
                logger.info("KRX 차단 기록 해제")
        except Exception as e:
            logger.warning(f"차단 기록 삭제 실패(무시): {e}")

    async def _require_login_iframe(self, page):
        """로그인 iframe을 집되, 없으면 왜 없는지 먼저 밝힌다.

        차단 페이지에도 iframe은 없다. 그래서 "iframe을 못 찾았다"만 남기면
        차단과 페이지 구조 변경이 같은 메시지로 보이고, 실제로 그 때문에
        2026-08-04 장애가 '카카오 로그인 구조 변경'으로 오진됐다.

        아울러 `query_selector`는 기다리지 않는다. iframe이 늦게 주입되면
        정상 페이지에서도 None이 나오므로, 짧게 기다려 본 뒤에 판정한다.
        """
        try:
            await page.wait_for_selector("iframe", timeout=self.LOGIN_WAIT_TIMEOUT)
        except Exception:
            pass

        iframe = await page.query_selector("iframe")
        if iframe:
            return iframe

        try:
            title = await page.title()
        except Exception:
            title = ""
        try:
            body = await page.inner_text("body")
        except Exception:
            body = ""

        if self._looks_blocked(title, body):
            reason = f"title={title!r} body={' '.join(body.split())[:200]!r}"
            self._mark_blocked(reason)
            raise KRXBlockedError(
                "KRX가 이 IP의 접근을 제한했습니다(로그인 페이지 대신 이용 제한 안내). "
                "KRX 안내에 따르면 탐지일로부터 1일간 제한되며, 제한 중 재시도가 "
                "다시 탐지되면 제한이 재적용됩니다. 대량 조회가 필요하면 "
                "KRX Open API(https://openapi.krx.co.kr) 등 공식 경로를 사용하세요. "
                f"[{reason}]"
            )

        raise KRXAuthError(
            f"로그인 iframe을 찾을 수 없습니다. (title={title!r}, "
            f"body={' '.join(body.split())[:120]!r})"
        )

    def _get_recent_business_day(self) -> str:
        """가장 최근 영업일 반환 (세션 검증용)"""
        kr_holidays = KR()
        dt = date.today()

        # 장 시작 전(09:00 이전)이면 전일부터 탐색
        if datetime.now().hour < 9:
            dt -= timedelta(days=1)

        # 최대 10일 전까지 탐색
        for _ in range(10):
            # 주말 체크
            if dt.weekday() >= 5:
                dt -= timedelta(days=1)
                continue
            # 공휴일 체크
            if dt in kr_holidays:
                dt -= timedelta(days=1)
                continue
            # 연말(12/31), 노동절(5/1) 체크
            if (dt.month == 12 and dt.day == 31) or (dt.month == 5 and dt.day == 1):
                dt -= timedelta(days=1)
                continue
            return dt.strftime("%Y%m%d")
            dt -= timedelta(days=1)

        return dt.strftime("%Y%m%d")

    def _validate_session(self) -> bool:
        """세션 유효성 검증 (실제 API 호출로 체크)"""
        try:
            # 가장 최근 영업일 계산 (장 시작 전/휴일에도 동작)
            check_date = self._get_recent_business_day()

            # 간단한 API 호출로 세션 유효성 체크
            resp = self.session.post(
                "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
                data={
                    "bld": "dbms/MDC/STAT/standard/MDCSTAT03501",
                    "mktId": "STK",
                    "trdDd": check_date,
                },
                timeout=10
            )

            # 응답이 비어있거나 HTML인 경우 (로그인 필요)
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" in content_type:
                logger.info("HTML 응답 - 로그인 필요")
                return False

            if not resp.text.strip():
                logger.info("빈 응답 - 로그인 필요")
                return False

            try:
                data = resp.json()
            except:
                logger.info("JSON 파싱 실패 - 로그인 필요")
                return False

            # 로그아웃 상태 체크
            if isinstance(data, dict) and data.get("RESULT") == "LOGOUT":
                return False

            # 데이터가 있으면 성공
            if "output" in data or "OutBlock_1" in data:
                return True

            # 빈 데이터도 성공으로 처리 (휴일 등)
            if isinstance(data, dict):
                return True

            return False

        except Exception as e:
            logger.warning(f"세션 검증 실패: {e}")
            return False

    def _acquire_lock(self, lock_file, timeout: float) -> bool:
        """
        파일 락 획득 시도 (타임아웃 포함)

        Args:
            lock_file: 락 파일 핸들
            timeout: 타임아웃 (초)

        Returns:
            락 획득 성공 여부
        """
        start_time = time.time()
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return True
            except BlockingIOError:
                # 다른 프로세스가 락을 보유 중
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    logger.warning(f"락 획득 타임아웃 ({timeout}초)")
                    return False
                logger.debug(f"락 대기 중... ({elapsed:.1f}초)")
                time.sleep(1)  # 1초 간격으로 재시도
            except Exception as e:
                logger.warning(f"락 획득 실패: {e}")
                return False

    def login(self, force: bool = False) -> bool:
        """
        KRX 로그인 (자동 재시도 포함, 파일 락으로 동시 로그인 방지)

        로그인 방식은 login_method 속성에 따라 결정됩니다:
        - "krx": KRX 직접 ID/PW 로그인
        - "kakao": 카카오 SNS 로그인 (기본값)

        Args:
            force: True면 기존 세션 무시하고 재로그인

        Returns:
            로그인 성공 여부

        Raises:
            KRX2FARequiredError: 카카오 로그인 시 2차인증이 활성화된 경우
        """
        # 기존 세션 체크 (락 없이)
        if not force and self._load_session():
            # 최근 검증됐으면 재검증 생략 (다른 프로세스에서 검증한 경우 포함)
            now = datetime.now()
            if self._last_validated:
                elapsed = now - self._last_validated
                logger.debug(f"세션 검증 시간 체크: last_validated={self._last_validated}, elapsed={elapsed}, threshold={self.VALIDATION_SKIP_THRESHOLD}")
                if elapsed < self.VALIDATION_SKIP_THRESHOLD:
                    logger.info(f"최근 검증된 세션 사용 (검증 시각: {self._last_validated})")
                    return True
            else:
                logger.debug("last_validated가 없음 - 검증 필요")

            if self._validate_session():
                self._update_last_validated()  # 검증 성공 시 파일에 기록
                logger.info("기존 세션이 유효합니다.")
                return True
            logger.info("기존 세션이 만료되어 재로그인합니다.")

        # 재로그인이 필요한 경우, 파일 락을 획득하여 동시 로그인 방지
        return self._login_with_lock(force)

    def _login_with_lock(self, force: bool = False) -> bool:
        """파일 락을 사용한 로그인 (동시 로그인 방지)"""
        # 락 파일 생성/열기
        self.LOCK_PATH.touch(exist_ok=True)

        with open(self.LOCK_PATH, 'w') as lock_file:
            logger.debug("로그인 락 획득 시도...")

            if not self._acquire_lock(lock_file, self.LOCK_WAIT_TIMEOUT):
                # 락 획득 실패 - 타임아웃
                raise KRXAuthError("로그인 락 획득 타임아웃 - 다른 프로세스가 로그인 중일 수 있음")

            logger.debug("로그인 락 획득 성공")

            try:
                # 락 획득 후 다시 세션 체크 (다른 프로세스가 로그인했을 수 있음)
                if not force and self._load_session():
                    now = datetime.now()
                    if self._last_validated:
                        elapsed = now - self._last_validated
                        if elapsed < self.VALIDATION_SKIP_THRESHOLD:
                            logger.info(f"다른 프로세스가 로그인 완료 - 세션 재사용 (검증 시각: {self._last_validated})")
                            return True

                    if self._validate_session():
                        self._update_last_validated()
                        logger.info("다른 프로세스가 로그인 완료 - 세션 유효")
                        return True

                # 차단 쿨다운 확인 — 재시도가 차단을 연장시키므로 시도 자체를
                # 건너뛴다. 세션 재사용 경로를 모두 지나온 뒤에 검사하는 이유는,
                # 차단 중이라도 살아있는 세션이 있으면 그건 계속 써도 되기 때문이다.
                blocked_until = self._blocked_until()
                if blocked_until:
                    remaining = (blocked_until - datetime.now()).total_seconds()
                    raise KRXBlockedError(
                        f"KRX 접근이 차단된 상태입니다. "
                        f"{remaining / 60:.0f}분 뒤({blocked_until:%H:%M})에 다시 시도하세요. "
                        f"차단 중 재시도는 차단을 연장시킵니다."
                    )

                # 세션 파일 정리 후 로그인
                self._cleanup_session_files()

                # 로그인 방식 선택
                login_method_name = "KRX 직접" if self.login_method == "krx" else "카카오"
                logger.info(f"{login_method_name} 로그인 시작...")

                # Playwright로 로그인 (재시도 로직 포함)
                last_error = None
                for attempt in range(self.MAX_LOGIN_RETRIES):
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            # 로그인 방식에 따라 다른 메서드 호출
                            if self.login_method == "krx":
                                result = loop.run_until_complete(self._login_async_krx())
                            else:
                                result = loop.run_until_complete(self._login_async_kakao())
                            if result:
                                # 들어왔다는 건 차단이 풀렸다는 뜻이다.
                                self._clear_blocked()
                                return True
                        finally:
                            loop.close()
                    except KRX2FARequiredError:
                        # 2FA 에러는 재시도해도 의미 없음 (카카오 로그인만 해당)
                        raise
                    except KRXBlockedError:
                        # 차단은 자격증명 문제가 아니라 상대가 우리를 막은 것이다.
                        # 더 두드려도 풀리지 않고 차단만 길어진다. 즉시 포기한다.
                        raise
                    except Exception as e:
                        last_error = e
                        logger.warning(f"로그인 시도 {attempt + 1}/{self.MAX_LOGIN_RETRIES} 실패: {e}")
                        if attempt < self.MAX_LOGIN_RETRIES - 1:
                            # 기본 대기 시간 + 랜덤 jitter (동시 로그인 경쟁 방지)
                            base_wait = (attempt + 1) * 20  # 20초, 40초, 60초, 80초...
                            jitter = random.uniform(5, 15)  # 5~15초 랜덤 추가
                            wait_time = base_wait + jitter
                            logger.info(f"{wait_time:.1f}초 후 재시도... (base={base_wait}s, jitter={jitter:.1f}s)")
                            time.sleep(wait_time)
                            self._cleanup_session_files()

                raise KRXAuthError(f"로그인 실패 (최대 재시도 횟수 초과): {last_error}")

            finally:
                # 락 해제 (with 문 종료 시 자동 해제되지만 명시적으로)
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    logger.debug("로그인 락 해제")
                except:
                    pass

    async def _login_async_kakao(self) -> bool:
        """비동기 카카오 SNS 로그인 처리"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise KRXAuthError(
                "playwright가 설치되지 않았습니다.\n"
                "'pip install playwright && playwright install chromium'을 실행하세요."
            )

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)

        context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
        )

        page = await context.new_page()

        # JavaScript alert/confirm/prompt 다이얼로그 자동 처리
        async def handle_dialog(dialog):
            logger.info(f"다이얼로그 감지: {dialog.type} - {dialog.message}")
            await dialog.accept()

        page.on('dialog', lambda dialog: asyncio.create_task(handle_dialog(dialog)))

        try:
            # KRX 로그인 페이지
            login_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
            await page.goto(login_url, wait_until="networkidle", timeout=self.PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(2)

            # iframe에서 카카오 로그인 버튼 클릭
            iframe = await self._require_login_iframe(page)

            frame = await iframe.content_frame()
            kakao_btn = await frame.wait_for_selector(
                'a.ms-kakao, a:has-text("카카오로 로그인")',
                timeout=self.LOGIN_WAIT_TIMEOUT
            )
            await kakao_btn.click()

            # 카카오 로그인 페이지 대기
            await page.wait_for_url("**/accounts.kakao.com/**", timeout=self.LOGIN_WAIT_TIMEOUT)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(1)

            # 아이디/비밀번호 입력
            await page.fill('input[name="loginId"], input#loginId', self.kakao_id)
            await asyncio.sleep(0.3)
            await page.fill('input[name="password"], input#password', self.kakao_pw)
            await asyncio.sleep(0.3)

            # 로그인 버튼 클릭
            await page.click('button[type="submit"], button.submit')
            logger.info("로그인 버튼 클릭됨. 2FA 확인 대기 중...")

            # 로그인 결과 대기 (최대 120초 - 2FA 확인 시간 포함)
            two_fa_detected = False
            krx_redirected = False
            for i in range(120):
                await asyncio.sleep(1)
                current_url = page.url

                # KRX로 리다이렉트 성공
                if current_url.startswith("http://data.krx.co.kr") or \
                   current_url.startswith("https://data.krx.co.kr"):
                    logger.info("KRX 로그인 성공!")
                    krx_redirected = True
                    break

                if i % 10 == 0:
                    logger.info(f"2FA 확인 대기 중... ({i}초)")

                # "계속하기" 버튼 처리 (2FA 후 나타남)
                try:
                    continue_btn = await page.query_selector('button:has-text("계속하기")')
                    if continue_btn:
                        logger.info("'계속하기' 버튼 발견, 클릭...")
                        await continue_btn.click()
                        await asyncio.sleep(2)
                        continue
                except:
                    pass

                # 동의 화면 처리
                try:
                    agree_btn = await page.query_selector(
                        'button:has-text("동의하고 계속하기"), button:has-text("전체 동의")'
                    )
                    if agree_btn:
                        logger.info("동의 버튼 클릭...")
                        await agree_btn.click()
                        await asyncio.sleep(2)
                except:
                    pass

            if not krx_redirected:
                # 2차인증 화면 감지
                try:
                    tfa_indicators = [
                        'text="카카오톡으로 인증"',
                        'text="인증 요청"',
                        'text="본인확인"',
                        'text="2단계 인증"',
                    ]
                    for indicator in tfa_indicators:
                        elem = await page.query_selector(indicator)
                        if elem:
                            two_fa_detected = True
                            break
                except:
                    pass

            # 2차인증 감지 시 에러 발생
            if two_fa_detected:
                await self._cleanup_browser()
                raise KRX2FARequiredError()

            # 로그인 성공 확인
            if not krx_redirected:
                current_url = page.url
                await self._cleanup_browser()
                raise KRXAuthError(
                    f"로그인 실패. 2FA 확인 또는 인증 정보를 확인하세요.\n"
                    f"현재 URL: {current_url[:100]}..."
                )

            # 쿠키 추출 및 저장
            cookies = await context.cookies()
            cookie_dict = {}
            krx_cookies = []

            # KRX 관련 쿠키만 필터링 및 적용
            for cookie in cookies:
                name = cookie["name"]
                value = cookie["value"]
                domain = cookie.get("domain", "")
                path = cookie.get("path", "/")

                # KRX 도메인 쿠키만 저장 (카카오 쿠키는 제외)
                if "krx.co.kr" in domain:
                    logger.debug(f"KRX 쿠키 발견: {name}={value[:20]}..., domain={domain}")
                    self.session.cookies.set(
                        name, value,
                        domain=domain,
                        path=path
                    )
                    cookie_dict[name] = value
                    krx_cookies.append({"name": name, "value": value, "domain": domain, "path": path})

            logger.info(f"KRX 쿠키 {len(krx_cookies)}개 저장됨")

            self._session_info = SessionInfo(
                cookies=cookie_dict,
                last_login=datetime.now()
            )

            self._save_session(cookie_dict, krx_cookies=krx_cookies)

            logger.info("카카오 로그인 성공")
            return True

        except KRX2FARequiredError:
            raise
        except (KRXBlockedError, KRX2FARequiredError):
            # 재시도해도 소용없는 실패는 그대로 올려보낸다. 아래 except 가
            # 이것들을 일반 KRXAuthError 로 감싸면 재시도 루프가 판별하지 못하고
            # 제한 중에도 다시 두드리게 된다 — 실서버에서 그렇게 재현됐다.
            raise
        except Exception as e:
            logger.error(f"카카오 로그인 실패: {e}")
            raise KRXAuthError(f"카카오 로그인 실패: {e}")
        finally:
            await self._cleanup_browser()

    async def _login_async_krx(self) -> bool:
        """비동기 KRX 직접 ID/PW 로그인 처리"""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            raise KRXAuthError(
                "playwright가 설치되지 않았습니다.\n"
                "'pip install playwright && playwright install chromium'을 실행하세요."
            )

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)

        context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="ko-KR",
        )

        page = await context.new_page()

        # JavaScript alert/confirm/prompt 다이얼로그 자동 처리
        async def handle_dialog(dialog):
            logger.info(f"다이얼로그 감지: {dialog.type} - {dialog.message}")
            await dialog.accept()

        page.on('dialog', lambda dialog: asyncio.create_task(handle_dialog(dialog)))

        try:
            # 기존 세션 정리를 위해 먼저 로그아웃 수행
            # (기존 세션이 있으면 새 로그인 후 mdc.client_session 쿠키가 발급되지 않음)
            logout_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001D2.cmd"
            logger.info(f"기존 세션 정리를 위해 로그아웃 수행: {logout_url}")
            await page.goto(logout_url, wait_until="networkidle", timeout=self.PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(10)  # KRX 서버에서 세션 정리 시간 확보 (충분히 대기)

            # KRX 로그인 페이지로 이동
            login_url = "https://data.krx.co.kr/contents/MDC/COMS/client/MDCCOMS001.cmd"
            await page.goto(login_url, wait_until="networkidle", timeout=self.PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(2)

            # iframe에서 로그인 폼 접근
            iframe = await self._require_login_iframe(page)

            frame = await iframe.content_frame()

            # KRX 아이디 입력 (iframe 내 #mbrId)
            id_input = await frame.wait_for_selector('#mbrId', timeout=self.LOGIN_WAIT_TIMEOUT)
            await id_input.fill(self.krx_id)
            logger.info(f"KRX 아이디 입력 완료: {self.krx_id[:3]}***")
            await asyncio.sleep(0.3)

            # 비밀번호 입력 (iframe 내 input[name="pw"])
            pw_input = await frame.wait_for_selector('input[name="pw"]', timeout=self.LOGIN_WAIT_TIMEOUT)
            await pw_input.fill(self.krx_pw)
            logger.info("KRX 비밀번호 입력 완료")
            await asyncio.sleep(0.3)

            # 로그인 버튼 클릭 (iframe 내 a.jsLoginBtn)
            login_btn = await frame.wait_for_selector('a.jsLoginBtn', timeout=self.LOGIN_WAIT_TIMEOUT)
            await login_btn.click()
            logger.info("KRX 로그인 버튼 클릭됨")

            # 로그인 처리 대기 (3초)
            await asyncio.sleep(3)

            # KRX 홈 페이지로 명시적 이동하여 로그인 상태 확인
            # (로그인 성공했어도 로그인 페이지로 리다이렉트되는 KRX 버그 대응)
            home_url = "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd"
            logger.info(f"홈 페이지로 이동하여 로그인 상태 확인: {home_url}")
            await page.goto(home_url, wait_until="networkidle", timeout=self.PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(2)

            # 홈 페이지에서 먼저 로그인 상태 확인
            current_url = page.url
            if "MDCCOMS001" in current_url:
                # 로그인 페이지로 리다이렉트됨 = 로그인 실패
                logger.warning(f"로그인 실패 - 홈 페이지에서 로그인 페이지로 리다이렉트됨: {current_url}")
                await self._cleanup_browser()
                raise KRXAuthError(
                    f"KRX 직접 로그인 실패. 아이디/비밀번호를 확인하세요.\n"
                    f"현재 URL: {current_url[:100]}..."
                )

            logger.info(f"KRX 직접 로그인 성공! 현재 URL: {current_url}")

            # 로그인 성공 후 서버 측 세션 안정화 대기 (동시 로그인 경쟁 방지를 위해 랜덤 jitter 추가)
            stabilization_wait = 5 + random.uniform(2, 8)  # 7~13초 랜덤 대기
            logger.info(f"세션 안정화 대기: {stabilization_wait:.1f}초...")
            await asyncio.sleep(stabilization_wait)

            # 데이터 조회 페이지로 이동하여 mdc.client_session 쿠키 발급 유도
            # (mdc.client_session 쿠키는 데이터 조회 페이지에서만 발급됨)
            data_page_url = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010105"
            logger.info(f"데이터 조회 페이지로 이동하여 mdc.client_session 쿠키 발급: {data_page_url}")
            await page.goto(data_page_url, wait_until="networkidle", timeout=self.PAGE_LOAD_TIMEOUT)
            await asyncio.sleep(3)  # mdc.client_session 쿠키 설정 대기

            # 데이터 조회 페이지에서 리다이렉트되면 세션이 무효화된 것임
            # (다른 프로세스에서 로그인하여 기존 세션이 만료됨)
            current_url = page.url
            if "MDCCOMS001" in current_url:
                logger.warning(
                    f"데이터 조회 페이지에서 로그인 페이지로 리다이렉트됨: {current_url}. "
                    "다른 프로세스에서 로그인하여 세션이 무효화되었을 수 있습니다. "
                    "브라우저를 재시작하고 재시도합니다."
                )
                await self._cleanup_browser()
                raise KRXSessionExpiredError(
                    "데이터 조회 페이지에서 로그인 페이지로 리다이렉트됨. "
                    "다른 프로세스의 로그인으로 세션이 무효화되었습니다."
                )

            # 쿠키 추출 및 저장 (재시도 로직 포함)
            cookies = []
            max_cookie_retries = 3

            for retry in range(max_cookie_retries):
                cookies = []

                # 1. Playwright context에서 쿠키 가져오기
                context_cookies = await context.cookies(["https://data.krx.co.kr"])
                cookies.extend(context_cookies)

                # 디버깅: context 쿠키 출력
                context_cookie_info = [(c["name"], c.get("domain", "")) for c in context_cookies]
                logger.info(f"[시도 {retry+1}/{max_cookie_retries}] Context 쿠키 {len(context_cookies)}개: {context_cookie_info}")

                # 2. JavaScript로 document.cookie에서 쿠키 가져오기 (mdc.client_session 포함)
                js_cookies_str = await page.evaluate("document.cookie")
                logger.info(f"[시도 {retry+1}/{max_cookie_retries}] JavaScript 쿠키: {js_cookies_str}")

                # JavaScript 쿠키 파싱
                if js_cookies_str:
                    for cookie_pair in js_cookies_str.split(';'):
                        cookie_pair = cookie_pair.strip()
                        if '=' in cookie_pair:
                            name, value = cookie_pair.split('=', 1)
                            name = name.strip()
                            value = value.strip()
                            if name and value:
                                # 중복 방지
                                if not any(c["name"] == name for c in cookies):
                                    cookies.append({
                                        "name": name,
                                        "value": value,
                                        "domain": "data.krx.co.kr",
                                        "path": "/"
                                    })
                                    logger.info(f"JS에서 추가 쿠키 발견: {name}")

                all_cookie_names = [c["name"] for c in cookies]
                logger.info(f"[시도 {retry+1}/{max_cookie_retries}] 총 {len(cookies)}개 쿠키: {all_cookie_names}")

                # mdc.client_session 쿠키가 있으면 성공
                if "mdc.client_session" in all_cookie_names:
                    logger.info("mdc.client_session 쿠키 확인됨!")
                    break

                # 마지막 시도가 아니면 페이지 새로고침 후 재시도
                if retry < max_cookie_retries - 1:
                    logger.warning(f"mdc.client_session 쿠키 미발견. 페이지 새로고침 후 재시도...")
                    await page.reload(wait_until="networkidle")
                    await asyncio.sleep(3)

            # mdc.client_session이 없으면 세션 만료로 처리하여 retry 로직이 재시도하도록 함
            # (경고만 하고 계속 진행하면 API 호출 시 즉시 LOGOUT 발생하여 무한 재로그인 루프)
            final_cookie_names = [c["name"] for c in cookies]
            if "mdc.client_session" not in final_cookie_names:
                logger.warning(
                    "⚠️ mdc.client_session 쿠키를 찾지 못했습니다. "
                    "KRX 서버 측 세션 충돌일 수 있습니다. 재시도합니다."
                )
                await self._cleanup_browser()
                raise KRXSessionExpiredError(
                    "mdc.client_session 쿠키를 찾지 못했습니다. "
                    "KRX 서버 측 세션이 정리되지 않았을 수 있습니다."
                )

            cookie_dict = {}
            krx_cookies = []

            # KRX 관련 쿠키만 필터링 및 적용
            for cookie in cookies:
                name = cookie["name"]
                value = cookie["value"]
                domain = cookie.get("domain", "")
                path = cookie.get("path", "/")

                # KRX 도메인 쿠키 저장 (도메인 체크 완화)
                if "krx.co.kr" in domain or domain == "":
                    logger.debug(f"KRX 쿠키 발견: {name}={value[:20]}..., domain={domain}")
                    self.session.cookies.set(
                        name, value,
                        domain=".krx.co.kr" if domain == "" else domain,
                        path=path
                    )
                    cookie_dict[name] = value
                    krx_cookies.append({"name": name, "value": value, "domain": domain, "path": path})

            cookie_names = [c["name"] for c in krx_cookies]
            logger.info(f"KRX 쿠키 {len(krx_cookies)}개 저장됨: {cookie_names}")

            self._session_info = SessionInfo(
                cookies=cookie_dict,
                last_login=datetime.now()
            )

            self._save_session(cookie_dict, krx_cookies=krx_cookies)

            logger.info("KRX 직접 로그인 성공")
            return True

        except (KRXBlockedError, KRX2FARequiredError):
            # 재시도해도 소용없는 실패는 그대로 올려보낸다. 아래 except 가
            # 이것들을 일반 KRXAuthError 로 감싸면 재시도 루프가 판별하지 못하고
            # 제한 중에도 다시 두드리게 된다 — 실서버에서 그렇게 재현됐다.
            raise
        except Exception as e:
            logger.error(f"KRX 직접 로그인 실패: {e}")
            raise KRXAuthError(f"KRX 직접 로그인 실패: {e}")
        finally:
            await self._cleanup_browser()

    async def _cleanup_browser(self):
        """브라우저 정리"""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    def _needs_refresh(self) -> bool:
        """세션 선제적 갱신이 필요한지 확인"""
        if not self._session_info:
            return True

        elapsed = datetime.now() - self._session_info.last_login
        return elapsed >= self.SESSION_REFRESH_THRESHOLD

    def check_session(self) -> bool:
        """
        세션 상태 체크 및 필요시 재로그인

        Returns:
            세션 유효 여부
        """
        if not self.is_logged_in:
            return self.login()

        # 선제적 갱신: 만료되기 전에 미리 갱신
        if self._needs_refresh():
            logger.info("세션 만료 예정, 선제적 갱신 시도...")
            if not self._validate_session():
                logger.info("세션이 만료되어 재로그인합니다.")
                self._cleanup_session_files()
                return self.login(force=True)

        if not self._validate_session():
            logger.info("세션이 만료되어 재로그인합니다.")
            self._cleanup_session_files()
            return self.login(force=True)

        return True


# 하위 호환성을 위한 별칭 (기존 코드에서 KakaoAuthManager를 참조하는 경우)
KakaoAuthManager = KRXAuthManager


class KRXDataClient:
    """
    KRX 데이터 클라이언트

    pykrx와 동일한 인터페이스로 KRX Data Marketplace에서 데이터를 조회합니다.
    KRX 직접 로그인 또는 카카오 SNS 로그인을 지원합니다.

    환경변수로 로그인 정보를 설정하는 방법:
    - KRX 직접 로그인: KRX_ID, KRX_PW (기본값)
    - 카카오 로그인: KRX_LOGIN_METHOD=kakao, KAKAO_ID, KAKAO_PW
    """

    API_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    ISIN_CACHE_PATH = Path.home() / ".krx_isin_cache.json"  # ISIN 캐시 파일
    ISIN_CACHE_TTL = timedelta(hours=12)  # 캐시 유효 시간

    # bld 파라미터 (pykrx 분석 결과)
    BLD = {
        # 종목 검색
        "finder_stkisu": "dbms/comm/finder/finder_stkisu",
        # 개별종목 시세 (OHLCV)
        "ohlcv": "dbms/MDC/STAT/standard/MDCSTAT01701",
        # 전종목 시세
        "ohlcv_all": "dbms/MDC/STAT/standard/MDCSTAT01501",
        # PER/PBR - 전종목
        "fundamental_all": "dbms/MDC/STAT/standard/MDCSTAT03501",
        # PER/PBR - 개별종목 기간조회
        "fundamental": "dbms/MDC/STAT/standard/MDCSTAT03502",
        # 투자자별 거래 - 기간합계
        "investor_summary": "dbms/MDC/STAT/standard/MDCSTAT02301",
        # 투자자별 거래 - 일별추이 (일반: 5개 투자자 유형)
        "investor_daily": "dbms/MDC/STAT/standard/MDCSTAT02302",
        # 투자자별 거래 - 일별추이 (상세: 12개 투자자 유형)
        "investor_daily_detail": "dbms/MDC/STAT/standard/MDCSTAT02303",
        # 지수 시세
        "index_ohlcv": "dbms/MDC/STAT/standard/MDCSTAT00301",
        # 지수 검색
        "finder_index": "dbms/comm/finder/finder_equidx",
        # 업종분류 현황
        "sector_classification": "dbms/MDC/STAT/standard/MDCSTAT03901",
    }

    # 시장 코드 매핑
    MARKET_CODE = {
        "KOSPI": "STK",
        "KOSDAQ": "KSQ",
        "KONEX": "KNX",
        "ALL": "ALL",
    }

    def __init__(
        self,
        kakao_id: Optional[str] = None,
        kakao_pw: Optional[str] = None,
        krx_id: Optional[str] = None,
        krx_pw: Optional[str] = None,
        login_method: Optional[str] = None,
        headless: bool = True,
        auto_login: bool = True,
    ):
        """
        클라이언트 초기화

        Args:
            kakao_id: 카카오 아이디 (login_method="kakao" 일 때 필요)
            kakao_pw: 카카오 비밀번호 (login_method="kakao" 일 때 필요)
            krx_id: KRX 직접 로그인 아이디 (login_method="krx" 일 때 필요)
            krx_pw: KRX 직접 로그인 비밀번호 (login_method="krx" 일 때 필요)
            login_method: 로그인 방식 ("krx" 또는 "kakao", 기본값: "krx")
            headless: 헤드리스 브라우저 모드
            auto_login: 자동 로그인 여부
        """
        self._auth_manager = KRXAuthManager(
            kakao_id=kakao_id,
            kakao_pw=kakao_pw,
            krx_id=krx_id,
            krx_pw=krx_pw,
            login_method=login_method,
            headless=headless,
        )

        # ticker → ISIN 캐시
        self._isin_cache: Dict[str, str] = {}
        self._isin_cache_date: Optional[str] = None

        if auto_login:
            self._auth_manager.login()

    @property
    def session(self) -> requests.Session:
        """requests 세션"""
        return self._auth_manager.session

    def _ensure_session(self):
        """
        세션 유효성 확인 - 클라이언트는 이 메서드를 직접 호출할 필요 없음

        자동으로 처리되는 것들:
        1. 프로세스 내 캐시된 세션 재사용 (5분간)
        2. 파일에 저장된 세션 로드 및 재사용 (다른 프로세스가 검증한 세션)
        3. 세션 만료 시 자동 재로그인 (파일 락으로 동시 로그인 방지)
        """
        global _last_session_check_time

        # 1. 프로세스 내 캐시 확인 (가장 빠름)
        if _last_session_check_time and datetime.now() - _last_session_check_time < FRESH_SESSION_THRESHOLD:
            return

        # 2. 파일에서 세션 로드 (다른 프로세스가 검증한 세션 재사용)
        #    login() 메서드가 파일 기반 세션 공유, 파일 락, 자동 재로그인 모두 처리함
        if not self._auth_manager.login():
            raise KRXSessionExpiredError("세션을 복구할 수 없습니다.")

        # 검증 성공 시간 기록
        _last_session_check_time = datetime.now()

    def _request(
        self,
        bld: str,
        params: Dict[str, Any],
        output_key: str = "output"
    ) -> List[Dict[str, Any]]:
        """
        KRX API 요청

        Args:
            bld: bld 파라미터
            params: 요청 파라미터
            output_key: 응답에서 데이터를 추출할 키

        Returns:
            응답 데이터 리스트
        """
        self._ensure_session()

        request_data = {"bld": bld, **params}

        try:
            resp = self.session.post(self.API_URL, data=request_data, timeout=30)
            resp.raise_for_status()

            data = resp.json()

            # 로그아웃 상태 체크
            if isinstance(data, dict):
                if data.get("RESULT") == "LOGOUT":
                    raise KRXSessionExpiredError("세션이 만료되었습니다.")

                # 데이터 추출
                if output_key in data:
                    return data[output_key]
                elif "OutBlock_1" in data:
                    return data["OutBlock_1"]
                elif "block1" in data:
                    return data["block1"]
                else:
                    return [data] if data else []
            elif isinstance(data, list):
                return data
            else:
                return []

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                # 400 에러의 실제 원인 파악
                response_text = e.response.text[:200] if e.response.text else "(empty)"
                content_type = e.response.headers.get("Content-Type", "")
                logger.warning(f"400 Bad Request - 응답: {response_text}, Content-Type: {content_type}")
                logger.debug(f"요청 파라미터: {request_data}")

                # 세션 문제 여부 판단
                is_session_issue = False
                if "text/html" in content_type:
                    # HTML 응답 = 로그인 페이지로 리다이렉트됨
                    is_session_issue = True
                    logger.info("HTML 응답 감지 - 세션 만료로 판단")
                elif not e.response.text or e.response.text.strip() == "":
                    # 빈 응답 = 세션 문제일 가능성
                    is_session_issue = True
                    logger.info("빈 응답 감지 - 세션 만료로 판단")
                elif "LOGOUT" in response_text.upper():
                    is_session_issue = True
                    logger.info("LOGOUT 감지 - 세션 만료로 판단")

                if is_session_issue:
                    raise KRXSessionExpiredError(f"세션 만료: {response_text}")
                else:
                    # 세션 문제가 아닌 400 에러 (잘못된 파라미터 등)
                    raise KRXDataError(f"API 요청 실패 (400): {response_text}")
            raise KRXDataError(f"API 요청 실패: {e}")
        except requests.exceptions.RequestException as e:
            raise KRXDataError(f"API 요청 실패: {e}")

    # =========================================================================
    # 종목 검색
    # =========================================================================

    @retry_on_session_expired()
    def get_market_ticker_list(
        self,
        date: Optional[str] = None,
        market: str = "ALL"
    ) -> List[str]:
        """
        종목 코드 리스트 조회

        Args:
            date: 기준일자 (YYYYMMDD), None이면 오늘
            market: 시장 (KOSPI/KOSDAQ/KONEX/ALL)

        Returns:
            종목 코드 리스트
        """
        df = self._get_ticker_info(market)
        return df["short_code"].tolist() if not df.empty else []

    @retry_on_session_expired()
    def get_market_ticker_name(self, date: Optional[str] = None, market: str = "ALL") -> Dict[str, str]:
        """
        종목코드-종목명 매핑

        Args:
            date: 기준일자 (미사용, 호환성용)
            market: 시장

        Returns:
            {종목코드: 종목명} 딕셔너리
        """
        df = self._get_ticker_info(market)
        if df.empty:
            return {}
        return dict(zip(df["short_code"], df["codeName"]))

    def _get_ticker_info(self, market: str = "ALL") -> DataFrame:
        """종목 정보 조회 (내부용)"""
        mktsel = self.MARKET_CODE.get(market.upper(), "ALL")

        items = self._request(
            self.BLD["finder_stkisu"],
            {"locale": "ko_KR", "mktsel": mktsel, "searchText": "", "typeNo": 0}
        )

        if not items:
            return DataFrame()

        return DataFrame(items)

    def _load_isin_cache(self) -> bool:
        """파일에서 ISIN 캐시 로드"""
        try:
            if not self.ISIN_CACHE_PATH.exists():
                return False

            data = json.loads(self.ISIN_CACHE_PATH.read_text())
            cache_date = data.get("date")
            cache_time_str = data.get("cached_at")
            cache = data.get("cache", {})

            if not cache_date or not cache_time_str or not cache:
                return False

            # TTL 체크
            cached_at = datetime.fromisoformat(cache_time_str)
            if datetime.now() - cached_at > self.ISIN_CACHE_TTL:
                logger.debug("ISIN 캐시 만료 (TTL 초과)")
                return False

            self._isin_cache = cache
            self._isin_cache_date = cache_date
            logger.debug(f"ISIN 캐시 파일에서 로드: {len(cache)}개 종목 (날짜: {cache_date})")
            return True

        except Exception as e:
            logger.warning(f"ISIN 캐시 로드 실패: {e}")
            return False

    def _save_isin_cache(self, date: str):
        """ISIN 캐시를 파일에 저장"""
        try:
            data = {
                "date": date,
                "cached_at": datetime.now().isoformat(),
                "cache": self._isin_cache
            }
            self.ISIN_CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False))
            logger.debug(f"ISIN 캐시 파일 저장: {len(self._isin_cache)}개 종목")
        except Exception as e:
            logger.warning(f"ISIN 캐시 저장 실패: {e}")

    def _build_isin_cache(self, date: str):
        """ISIN 캐시 구축 (메모리 → 파일 → API 순서로 확인)"""
        # 1. 메모리 캐시 확인
        if self._isin_cache and self._isin_cache_date == date:
            return

        # 2. 파일 캐시 확인
        if self._load_isin_cache():
            if self._isin_cache_date == date:
                return
            # 날짜가 다르면 재구축 필요
            logger.debug(f"ISIN 캐시 날짜 불일치: {self._isin_cache_date} vs {date}")

        # 3. API에서 새로 구축
        logger.info(f"ISIN 캐시 구축 중... (날짜: {date})")
        items = self._request(
            self.BLD["fundamental_all"],
            {"mktId": "ALL", "trdDd": date}
        )

        self._isin_cache = {}
        for item in items:
            ticker = item.get("ISU_SRT_CD", "")
            isin = item.get("ISU_CD", "")
            if ticker and isin:
                self._isin_cache[ticker] = isin

        self._isin_cache_date = date
        logger.info(f"ISIN 캐시 구축 완료: {len(self._isin_cache)}개 종목")

        # 파일에 저장 (다른 프로세스가 재사용 가능)
        self._save_isin_cache(date)

    def _get_isin(self, ticker: str, date: str) -> Optional[str]:
        """ticker에서 ISIN 조회"""
        # ISIN 캐시는 가장 최근 영업일 기준으로 구축 (장 시작 전/휴일에도 동작)
        cache_date = self.get_nearest_business_day(date)
        self._build_isin_cache(cache_date)
        isin = self._isin_cache.get(ticker)

        # fundamental_all에서 못 찾으면 finder_stkisu에서 검색 (외국 상장사 등)
        if not isin:
            items = self._request(
                self.BLD["finder_stkisu"],
                {"locale": "ko_KR", "mktsel": "ALL", "searchText": ticker, "typeNo": 0}
            )
            for item in items:
                if item.get("short_code") == ticker:
                    isin = item.get("full_code")
                    # 캐시에 추가
                    if isin:
                        self._isin_cache[ticker] = isin
                        logger.debug(f"finder_stkisu에서 ISIN 찾음: {ticker} -> {isin}")
                    break

        return isin

    # =========================================================================
    # OHLCV (시세)
    # =========================================================================

    @retry_on_session_expired()
    def get_market_ohlcv(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        adjusted: bool = True
    ) -> DataFrame:
        """
        개별종목 OHLCV 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드 (6자리)
            adjusted: 수정주가 여부

        Returns:
            DataFrame: 날짜별 OHLCV
                - Open, High, Low, Close, Volume
        """
        isin = self._get_isin(ticker, todate)
        if not isin:
            raise KRXDataError(f"종목을 찾을 수 없습니다: {ticker}")

        items = self._request(
            self.BLD["ohlcv"],
            {
                "isuCd": isin,
                "strtDd": fromdate,
                "endDd": todate,
                "adjStkPrc": 2 if adjusted else 1,  # 2: 수정주가, 1: 단순주가
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (pykrx 형식)
        column_map = {
            "TRD_DD": "날짜",
            "TDD_OPNPRC": "시가",
            "TDD_HGPRC": "고가",
            "TDD_LWPRC": "저가",
            "TDD_CLSPRC": "종가",
            "ACC_TRDVOL": "거래량",
            "ACC_TRDVAL": "거래대금",
            "MKTCAP": "시가총액",
        }
        df = df.rename(columns=column_map)

        # pykrx 영문 컬럼명으로 변환
        eng_map = {
            "시가": "Open",
            "고가": "High",
            "저가": "Low",
            "종가": "Close",
            "거래량": "Volume",
            "거래대금": "Amount",
            "시가총액": "MarketCap",
        }
        df = df.rename(columns=eng_map)

        # 숫자 변환
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "Amount", "MarketCap"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 날짜 인덱스
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y/%m/%d")
            df = df.set_index("날짜")
            df.index.name = None
            df = df.sort_index()

        # pykrx와 동일한 컬럼만 반환
        result_cols = ["Open", "High", "Low", "Close", "Volume", "Amount", "MarketCap"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    # =========================================================================
    # 시가총액
    # =========================================================================

    @retry_on_session_expired()
    def get_market_cap(
        self,
        fromdate: str,
        todate: str,
        ticker: str
    ) -> DataFrame:
        """
        시가총액 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드

        Returns:
            DataFrame: 시가총액, 거래량, 거래대금, 상장주식수
        """
        df = self.get_market_ohlcv(fromdate, todate, ticker)

        if df.empty:
            return df

        # 시가총액 관련 컬럼만 반환
        cols = ["MarketCap", "Volume", "Amount"]
        available = [c for c in cols if c in df.columns]

        return df[available] if available else df

    # =========================================================================
    # PER/PBR/배당수익률 (Fundamental)
    # =========================================================================

    @retry_on_session_expired()
    def get_market_fundamental(
        self,
        fromdate: str,
        todate: str,
        ticker: str
    ) -> DataFrame:
        """
        PER/PBR/배당수익률 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드

        Returns:
            DataFrame: BPS, PER, PBR, EPS, DIV, DPS
        """
        isin = self._get_isin(ticker, todate)
        if not isin:
            raise KRXDataError(f"종목을 찾을 수 없습니다: {ticker}")

        items = self._request(
            self.BLD["fundamental"],
            {
                "isuCd": isin,
                "mktId": "ALL",
                "strtDd": fromdate,
                "endDd": todate,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (pykrx 형식)
        column_map = {
            "TRD_DD": "날짜",
            "TDD_CLSPRC": "종가",
            "EPS": "EPS",
            "PER": "PER",
            "BPS": "BPS",
            "PBR": "PBR",
            "DPS": "DPS",
            "DVD_YLD": "DIV",
        }
        df = df.rename(columns=column_map)

        # 숫자 변환
        numeric_cols = ["종가", "EPS", "PER", "BPS", "PBR", "DPS", "DIV"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 날짜 인덱스
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y/%m/%d")
            df = df.set_index("날짜")
            df.index.name = None
            df = df.sort_index()

        # pykrx와 동일한 컬럼만 반환
        result_cols = ["BPS", "PER", "PBR", "EPS", "DIV", "DPS"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    # =========================================================================
    # 투자자별 거래량
    # =========================================================================

    @retry_on_session_expired()
    def get_market_trading_volume_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        detail: bool = False
    ) -> DataFrame:
        """
        투자자별 거래량 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드
            detail: 상세 투자자 구분 여부
                   - False: 5개 유형 (기관합계, 기타법인, 개인, 외국인합계, 전체)
                   - True: 12개 유형 (금융투자, 보험, 투신, 사모, 은행, 기타금융, 연기금, 기타법인, 개인, 외국인, 기타외국인, 전체)

        Returns:
            DataFrame: 투자자별 순매수량
        """
        isin = self._get_isin(ticker, todate)
        if not isin:
            raise KRXDataError(f"종목을 찾을 수 없습니다: {ticker}")

        # detail 여부에 따라 다른 bld 사용
        bld_key = "investor_daily_detail" if detail else "investor_daily"

        items = self._request(
            self.BLD[bld_key],
            {
                "isuCd": isin,
                "strtDd": fromdate,
                "endDd": todate,
                "inqTpCd": 2,
                "trdVolVal": 1,  # 거래량
                "askBid": 3,     # 순매수
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (detail 여부에 따라 다름)
        if detail:
            # 상세: 12개 투자자 유형
            column_map = {
                "TRD_DD": "날짜",
                "TRDVAL1": "금융투자",
                "TRDVAL2": "보험",
                "TRDVAL3": "투신",
                "TRDVAL4": "사모",
                "TRDVAL5": "은행",
                "TRDVAL6": "기타금융",
                "TRDVAL7": "연기금",
                "TRDVAL8": "기타법인",
                "TRDVAL9": "개인",
                "TRDVAL10": "외국인",
                "TRDVAL11": "기타외국인",
                "TRDVAL_TOT": "전체",
            }
        else:
            # 일반: 5개 투자자 유형
            column_map = {
                "TRD_DD": "날짜",
                "TRDVAL1": "기관합계",
                "TRDVAL2": "기타법인",
                "TRDVAL3": "개인",
                "TRDVAL4": "외국인합계",
                "TRDVAL_TOT": "전체",
            }

        df = df.rename(columns=column_map)

        # 숫자 변환
        numeric_cols = list(column_map.values())[1:]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 날짜 인덱스
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y/%m/%d")
            df = df.set_index("날짜")
            df.index.name = None
            df = df.sort_index()

        return df

    # =========================================================================
    # 지수 OHLCV
    # =========================================================================

    @retry_on_session_expired()
    def get_index_ohlcv(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        freq: str = "d"
    ) -> DataFrame:
        """
        지수 OHLCV 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 지수 코드 (예: 1001=KOSPI, 2001=KOSDAQ)
            freq: 빈도 (d/m/y) - 현재 d만 지원

        Returns:
            DataFrame: 지수 OHLCV
        """
        # pykrx 지수 티커 형식: 1xxx=KOSPI, 2xxx=KOSDAQ
        # API 파라미터:
        #   indIdx: 그룹 ID (1=KOSPI, 2=KOSDAQ 등)
        #   indIdx2: 지수 코드 (001=코스피/코스닥, 028=KOSPI 200 등)
        ticker_str = str(ticker)

        ind_idx = ticker_str[0]    # 첫 번째 자리: 그룹 ID
        idx_code = ticker_str[1:]  # 나머지: 지수 코드

        items = self._request(
            self.BLD["index_ohlcv"],
            {
                "indIdx2": idx_code,
                "indIdx": ind_idx,
                "strtDd": fromdate,
                "endDd": todate,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑
        column_map = {
            "TRD_DD": "날짜",
            "OPNPRC_IDX": "시가",
            "HGPRC_IDX": "고가",
            "LWPRC_IDX": "저가",
            "CLSPRC_IDX": "종가",
            "ACC_TRDVOL": "거래량",
            "ACC_TRDVAL": "거래대금",
        }
        df = df.rename(columns=column_map)

        # pykrx 영문 컬럼
        eng_map = {
            "시가": "Open",
            "고가": "High",
            "저가": "Low",
            "종가": "Close",
            "거래량": "Volume",
            "거래대금": "Amount",
        }
        df = df.rename(columns=eng_map)

        # 숫자 변환
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "Amount"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 날짜 인덱스
        if "날짜" in df.columns:
            df["날짜"] = pd.to_datetime(df["날짜"], format="%Y/%m/%d")
            df = df.set_index("날짜")
            df.index.name = None
            df = df.sort_index()

        # pykrx와 동일한 컬럼만 반환
        result_cols = ["Open", "High", "Low", "Close", "Volume", "Amount"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    # =========================================================================
    # 전체 종목 조회 (pykrx 호환)
    # =========================================================================

    @retry_on_session_expired()
    def get_market_ohlcv_by_ticker(self, date: str, market: str = "ALL") -> DataFrame:
        """
        특정일 전체 종목의 OHLCV 조회 (pykrx 호환)

        Args:
            date: 조회일 (YYYYMMDD)
            market: 시장구분 ("ALL", "KOSPI", "KOSDAQ", "KONEX")

        Returns:
            DataFrame: 종목코드 인덱스, OHLCV 컬럼
        """
        # 가장 최근 영업일로 변환 (장 시작 전/휴일에도 동작)
        query_date = self.get_nearest_business_day(date)

        market_map = {
            "ALL": "ALL",
            "KOSPI": "STK",
            "KOSDAQ": "KSQ",
            "KONEX": "KNX",
        }
        mkt_id = market_map.get(market.upper(), "ALL")

        items = self._request(
            "dbms/MDC/STAT/standard/MDCSTAT01501",
            {
                "mktId": mkt_id,
                "trdDd": query_date,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (pykrx 호환 영문 컬럼명)
        column_map = {
            "ISU_SRT_CD": "Ticker",
            "TDD_OPNPRC": "Open",
            "TDD_HGPRC": "High",
            "TDD_LWPRC": "Low",
            "TDD_CLSPRC": "Close",
            "ACC_TRDVOL": "Volume",
            "ACC_TRDVAL": "Amount",
        }
        df = df.rename(columns=column_map)

        # 숫자 변환
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "Amount"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 티커 인덱스
        if "Ticker" in df.columns:
            df = df.set_index("Ticker")

        # 필요한 컬럼만 반환 (pykrx 호환 영문 컬럼명)
        result_cols = ["Open", "High", "Low", "Close", "Volume", "Amount"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    @retry_on_session_expired()
    def get_market_cap_by_ticker(self, date: str, market: str = "ALL") -> DataFrame:
        """
        특정일 전체 종목의 시가총액 조회 (pykrx 호환)

        Args:
            date: 조회일 (YYYYMMDD)
            market: 시장구분 ("ALL", "KOSPI", "KOSDAQ", "KONEX")

        Returns:
            DataFrame: 종목코드 인덱스, 시가총액/거래량/거래대금/상장주식수 컬럼
        """
        # 가장 최근 영업일로 변환 (장 시작 전/휴일에도 동작)
        query_date = self.get_nearest_business_day(date)

        market_map = {
            "ALL": "ALL",
            "KOSPI": "STK",
            "KOSDAQ": "KSQ",
            "KONEX": "KNX",
        }
        mkt_id = market_map.get(market.upper(), "ALL")

        # MDCSTAT01501 (전종목 시세) endpoint 사용 - MKTCAP 포함
        items = self._request(
            self.BLD["ohlcv_all"],
            {
                "mktId": mkt_id,
                "trdDd": query_date,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 (pykrx 형식)
        column_map = {
            "ISU_SRT_CD": "티커",
            "MKTCAP": "시가총액",
            "ACC_TRDVOL": "거래량",
            "ACC_TRDVAL": "거래대금",
            "LIST_SHRS": "상장주식수",
        }
        df = df.rename(columns=column_map)

        # 숫자 변환
        numeric_cols = ["시가총액", "거래량", "거래대금", "상장주식수"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ""),
                    errors="coerce"
                )

        # 티커 인덱스
        if "티커" in df.columns:
            df = df.set_index("티커")

        # 필요한 컬럼만 반환
        result_cols = ["시가총액", "거래량", "거래대금", "상장주식수"]
        available = [c for c in result_cols if c in df.columns]

        return df[available] if available else df

    @retry_on_session_expired()
    def get_market_sector_info(self, date: str, market: str = "KOSPI") -> Dict[str, str]:
        """
        전체 종목의 업종분류 정보 조회

        Args:
            date: 조회일 (YYYYMMDD)
            market: 시장구분 ("KOSPI", "KOSDAQ")

        Returns:
            Dict[str, str]: {종목코드: 업종명} 매핑
            Example: {"005930": "전기전자", "000660": "전기전자", "005380": "운수장비"}
        """
        query_date = self.get_nearest_business_day(date)

        market_map = {
            "KOSPI": "STK",
            "KOSDAQ": "KSQ",
        }
        mkt_id = market_map.get(market.upper(), "STK")

        items = self._request(
            self.BLD["sector_classification"],
            {
                "mktId": mkt_id,
                "trdDd": query_date,
            },
            output_key="block1"
        )

        if not items:
            return {}

        result = {}
        for item in items:
            ticker = item.get("ISU_SRT_CD", "")
            sector = item.get("IDX_IND_NM", "")
            if ticker and sector:
                result[ticker] = sector

        logger.info(f"업종분류 정보 조회 완료: {len(result)}개 종목 ({market})")
        return result

    def get_market_ticker_list(self, date: Optional[str] = None, market: str = "KOSPI") -> List[str]:
        """
        티커 목록 조회 (pykrx 호환)

        Args:
            date: 조회일 (YYYYMMDD), None이면 최근 영업일
            market: 시장구분 ("KOSPI", "KOSDAQ", "KONEX", "ALL")

        Returns:
            List[str]: 티커 코드 리스트
        """
        if date is None:
            date = self.get_nearest_business_day()

        tickers = self.get_market_ticker_name(date=date, market=market)
        return list(tickers.keys())

    @retry_on_session_expired()
    def get_market_trading_value_by_investor(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        detail: bool = False
    ) -> DataFrame:
        """
        투자자별 거래대금 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드 (6자리)
            detail: True면 12개 투자자, False면 5개 합산

        Returns:
            DataFrame: 날짜별 투자자별 순매수금액
        """
        isin = self._get_isin(ticker, todate)
        if not isin:
            raise KRXDataError(f"종목을 찾을 수 없습니다: {ticker}")

        # detail 여부에 따라 다른 BLD 사용
        bld = self.BLD["trading_volume_detail"] if detail else self.BLD["trading_volume"]

        items = self._request(
            bld,
            {
                "isuCd": isin,
                "strtDd": fromdate,
                "endDd": todate,
            }
        )

        if not items:
            return DataFrame()

        df = DataFrame(items)

        # 컬럼 매핑 - 거래대금(ASK_TRDVAL: 매도, BID_TRDVAL: 매수)
        date_col = "TRD_DD"
        if date_col not in df.columns:
            return DataFrame()

        # 순매수금액 계산 (매수 - 매도)
        result_data = []
        for _, row in df.iterrows():
            row_dict = {"날짜": row[date_col]}

            if detail:
                # 상세 (12개 투자자)
                investors = [
                    ("금융투자", "TRDVAL1"),
                    ("보험", "TRDVAL2"),
                    ("투신", "TRDVAL3"),
                    ("사모", "TRDVAL4"),
                    ("은행", "TRDVAL5"),
                    ("기타금융", "TRDVAL6"),
                    ("연기금", "TRDVAL7"),
                    ("기관합계", "TRDVAL_INST"),
                    ("기타법인", "TRDVAL8"),
                    ("개인", "TRDVAL9"),
                    ("외국인", "TRDVAL10"),
                    ("기타외국인", "TRDVAL11"),
                ]
            else:
                # 합산 (5개 투자자)
                investors = [
                    ("기관합계", "TRDVAL_INST"),
                    ("기타법인", "TRDVAL1"),
                    ("개인", "TRDVAL2"),
                    ("외국인합계", "TRDVAL3"),
                    ("전체", "TRDVAL4"),
                ]

            for name, col_prefix in investors:
                buy_col = f"BID_{col_prefix}" if f"BID_{col_prefix}" in row else col_prefix
                sell_col = f"ASK_{col_prefix}" if f"ASK_{col_prefix}" in row else None

                if buy_col in row:
                    buy = pd.to_numeric(str(row.get(buy_col, 0)).replace(",", ""), errors="coerce") or 0
                    sell = pd.to_numeric(str(row.get(sell_col, 0)).replace(",", ""), errors="coerce") or 0 if sell_col else 0
                    row_dict[name] = buy - sell

            result_data.append(row_dict)

        result_df = DataFrame(result_data)

        # 날짜 인덱스
        if "날짜" in result_df.columns:
            result_df["날짜"] = pd.to_datetime(result_df["날짜"], format="%Y/%m/%d")
            result_df = result_df.set_index("날짜")
            result_df.index.name = None
            result_df = result_df.sort_index()

        return result_df

    @retry_on_session_expired()
    def get_market_trading_value_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        on: str = "순매수"
    ) -> DataFrame:
        """
        일자별 거래대금 조회 (pykrx 호환)

        Args:
            fromdate: 시작일 (YYYYMMDD)
            todate: 종료일 (YYYYMMDD)
            ticker: 종목코드 (6자리)
            on: "매도", "매수", "순매수" 중 하나

        Returns:
            DataFrame: 날짜별 거래대금
        """
        # 투자자별 거래대금 조회 후 집계
        df = self.get_market_trading_value_by_investor(fromdate, todate, ticker)
        return df

    # =========================================================================
    # pykrx 호환 별칭
    # =========================================================================

    def get_market_ohlcv_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        adjusted: bool = True
    ) -> DataFrame:
        """pykrx 호환: get_market_ohlcv의 별칭"""
        return self.get_market_ohlcv(fromdate, todate, ticker, adjusted)

    def get_market_cap_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str
    ) -> DataFrame:
        """pykrx 호환: get_market_cap의 별칭"""
        return self.get_market_cap(fromdate, todate, ticker)

    def get_market_fundamental_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str
    ) -> DataFrame:
        """pykrx 호환: get_market_fundamental의 별칭"""
        return self.get_market_fundamental(fromdate, todate, ticker)

    def get_index_ohlcv_by_date(
        self,
        fromdate: str,
        todate: str,
        ticker: str,
        freq: str = "d"
    ) -> DataFrame:
        """pykrx 호환: get_index_ohlcv의 별칭"""
        return self.get_index_ohlcv(fromdate, todate, ticker, freq)

    # =========================================================================
    # 유틸리티
    # =========================================================================

    def get_nearest_business_day(self, target_date: Optional[str] = None) -> str:
        """
        가장 가까운 영업일 조회 (과거 방향으로 탐색)

        Args:
            target_date: 기준일 (YYYYMMDD), None이면 오늘

        Returns:
            영업일 (YYYYMMDD)

        Note:
            - 장 시작 전(09:00 이전)에는 전 영업일을 반환
            - 장 마감 후에는 당일을 반환
            - 전달된 날짜가 오늘인 경우에도 동일하게 적용
        """
        today = date.today()

        if target_date:
            dt = datetime.strptime(target_date, "%Y%m%d").date()
        else:
            dt = today

        kr_holidays = KR()

        # 오늘 날짜인 경우, 장 시작 전(09:00 이전)이면 전일 기준으로 탐색
        if dt == today:
            now = datetime.now()
            if now.hour < 9:
                # 오늘이 영업일이어도 아직 데이터가 없으므로 전일부터 탐색
                dt -= timedelta(days=1)

        # 최대 10일 전까지 탐색
        for _ in range(10):
            if self._is_market_day(dt, kr_holidays):
                return dt.strftime("%Y%m%d")
            dt -= timedelta(days=1)

        return dt.strftime("%Y%m%d")

    def _is_market_day(self, dt: date, kr_holidays) -> bool:
        """
        한국 주식 시장 영업일 여부 확인

        Args:
            dt: 확인할 날짜
            kr_holidays: 한국 공휴일 객체

        Returns:
            영업일이면 True
        """
        # 주말
        if dt.weekday() >= 5:
            return False

        # 공휴일
        if dt in kr_holidays:
            return False

        # 노동절 (5/1) - 증권시장 휴장
        if dt.month == 5 and dt.day == 1:
            return False

        # 연말 (12/31) - 증권시장 휴장
        if dt.month == 12 and dt.day == 31:
            return False

        # 연도별 특별 휴장일
        if dt.year == 2025:
            special_holidays = [
                (1, 27),   # 설날 연휴 임시공휴일
                (3, 3),    # 삼일절 대체공휴일
                (5, 6),    # 어린이날/부처님오신날 대체공휴일
                (6, 3),    # 대통령선거일
                (10, 8),   # 추석 대체공휴일
            ]
            if (dt.month, dt.day) in special_holidays:
                return False

        return True

    def is_market_day(self, target_date: Optional[str] = None) -> bool:
        """
        특정 날짜가 영업일인지 확인

        Args:
            target_date: 확인할 날짜 (YYYYMMDD), None이면 오늘

        Returns:
            영업일이면 True
        """
        if target_date:
            dt = datetime.strptime(target_date, "%Y%m%d").date()
        else:
            dt = date.today()

        return self._is_market_day(dt, KR())

    def close(self):
        """리소스 정리"""
        pass  # 현재는 특별히 정리할 것 없음

    def get_nearest_business_day_in_a_week(
        self,
        target_date: Optional[str] = None,
        prev: bool = True
    ) -> str:
        """
        pykrx 호환: 일주일 내 가장 가까운 영업일 조회

        Args:
            target_date: 기준일 (YYYYMMDD), None이면 오늘
            prev: True면 과거 방향, False면 미래 방향

        Returns:
            영업일 (YYYYMMDD)
        """
        # 현재는 과거 방향만 지원 (prev=True)
        return self.get_nearest_business_day(target_date)


# =============================================================================
# 모듈 레벨 편의 객체 (pykrx 호환용)
# =============================================================================

# 싱글톤 클라이언트 (lazy initialization)
_default_client: Optional[KRXDataClient] = None
_last_session_check_time: Optional[datetime] = None

# 세션 검증 생략 임계값 (이 시간 내에 검증했으면 재검증 생략)
FRESH_SESSION_THRESHOLD = timedelta(minutes=5)


def _get_client() -> KRXDataClient:
    """기본 클라이언트 반환 (lazy initialization)"""
    global _default_client
    if _default_client is None:
        _default_client = KRXDataClient()
    return _default_client


def ensure_session_valid() -> bool:
    """
    [DEPRECATED] 세션 유효성 확인 - 호출할 필요 없음

    v0.3.19부터 모든 API 호출 시 자동으로 세션이 관리됩니다:
    - 세션 만료 시 자동 재로그인
    - 파일 락으로 동시 로그인 방지
    - 프로세스 간 세션 공유

    클라이언트는 그냥 데이터를 요청하면 됩니다.
    이 함수는 하위 호환성을 위해 유지되며, 호출해도 무해합니다.

    Returns:
        항상 True (세션이 자동으로 복구되므로)
    """
    global _last_session_check_time

    # 최근에 검증했으면 생략
    if _last_session_check_time and datetime.now() - _last_session_check_time < FRESH_SESSION_THRESHOLD:
        logger.debug(f"세션 최근 검증됨 ({_last_session_check_time}), 생략")
        return True

    try:
        client = _get_client()
        # 세션이 이미 로그인된 상태면 성공
        if client._auth_manager.is_logged_in:
            _last_session_check_time = datetime.now()
            logger.info("세션 프리워밍 완료 (기존 세션 사용)")
            return True

        # 세션 검증 및 필요시 로그인
        result = client._auth_manager.check_session()
        if result:
            _last_session_check_time = datetime.now()
            logger.info("세션 프리워밍 완료 (세션 검증/로그인)")
        return result
    except Exception as e:
        logger.error(f"세션 프리워밍 실패: {e}")
        return False


def is_session_fresh() -> bool:
    """세션이 최근에 검증되어 재검증이 불필요한지 확인"""
    global _last_session_check_time
    if _last_session_check_time is None:
        return False
    return datetime.now() - _last_session_check_time < FRESH_SESSION_THRESHOLD


def reset_session_freshness():
    """세션 freshness 리셋 (테스트용)"""
    global _last_session_check_time
    _last_session_check_time = None


# pykrx.stock.stock_api 호환 함수들
def get_market_ohlcv_by_date(fromdate: str, todate: str, ticker: str, adjusted: bool = True) -> DataFrame:
    """pykrx 호환: 개별종목 OHLCV"""
    return _get_client().get_market_ohlcv_by_date(fromdate, todate, ticker, adjusted)


def get_market_ohlcv_by_ticker(date: str, market: str = "ALL") -> DataFrame:
    """pykrx 호환: 특정일 전체 종목 OHLCV"""
    return _get_client().get_market_ohlcv_by_ticker(date, market)


def get_market_cap_by_ticker(date: str, market: str = "ALL") -> DataFrame:
    """pykrx 호환: 특정일 전체 종목 시가총액"""
    return _get_client().get_market_cap_by_ticker(date, market)


def get_market_cap_by_date(fromdate: str, todate: str, ticker: str) -> DataFrame:
    """pykrx 호환: 시가총액"""
    return _get_client().get_market_cap_by_date(fromdate, todate, ticker)


def get_market_fundamental_by_date(fromdate: str, todate: str, ticker: str) -> DataFrame:
    """pykrx 호환: 기본 지표"""
    return _get_client().get_market_fundamental_by_date(fromdate, todate, ticker)


def get_market_trading_volume_by_date(fromdate: str, todate: str, ticker: str, detail: bool = False) -> DataFrame:
    """pykrx 호환: 투자자별 거래량"""
    return _get_client().get_market_trading_volume_by_date(fromdate, todate, ticker, detail)


def get_market_trading_value_by_date(fromdate: str, todate: str, ticker: str, on: str = "순매수") -> DataFrame:
    """pykrx 호환: 일자별 거래대금"""
    return _get_client().get_market_trading_value_by_date(fromdate, todate, ticker, on)


def get_market_trading_volume_by_investor(fromdate: str, todate: str, ticker: str, detail: bool = False) -> DataFrame:
    """pykrx 호환: 투자자별 거래량"""
    return _get_client().get_market_trading_volume_by_date(fromdate, todate, ticker, detail)


def get_market_trading_value_by_investor(fromdate: str, todate: str, ticker: str, detail: bool = False) -> DataFrame:
    """pykrx 호환: 투자자별 거래대금"""
    return _get_client().get_market_trading_value_by_investor(fromdate, todate, ticker, detail)


def get_market_ticker_list(date: Optional[str] = None, market: str = "KOSPI") -> List[str]:
    """pykrx 호환: 티커 목록"""
    return _get_client().get_market_ticker_list(date, market)


def get_market_ticker_name(ticker: str) -> str:
    """pykrx 호환: 티커 이름"""
    client = _get_client()
    tickers = client.get_market_ticker_name(market="ALL")
    return tickers.get(ticker, "")


def get_index_ohlcv_by_date(fromdate: str, todate: str, ticker: str, freq: str = "d") -> DataFrame:
    """pykrx 호환: 지수 OHLCV"""
    return _get_client().get_index_ohlcv_by_date(fromdate, todate, ticker, freq)


def get_nearest_business_day_in_a_week(target_date: Optional[str] = None, prev: bool = True) -> str:
    """pykrx 호환: 일주일 내 가장 가까운 영업일"""
    return _get_client().get_nearest_business_day_in_a_week(target_date, prev)


# =============================================================================
# 테스트
# =============================================================================

def test_client():
    """클라이언트 테스트"""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    print("=" * 60)
    print("KRX Data Client 테스트")
    print("=" * 60)

    try:
        client = KRXDataClient()

        # 종목 코드 조회
        print("\n[1] 종목코드 조회")
        ticker_map = client.get_market_ticker_name(market="KOSPI")
        print(f"KOSPI 종목 수: {len(ticker_map)}")

        # OHLCV 조회
        print("\n[2] 삼성전자 OHLCV (2024-12-01 ~ 2024-12-20)")
        df = client.get_market_ohlcv("20241201", "20241220", "005930")
        print(df.head())

        # PER/PBR 조회
        print("\n[3] 삼성전자 PER/PBR (2024-12-01 ~ 2024-12-20)")
        df = client.get_market_fundamental("20241201", "20241220", "005930")
        print(df.head())

        # 투자자별 거래량
        print("\n[4] 삼성전자 투자자별 거래량 (2024-12-01 ~ 2024-12-20)")
        df = client.get_market_trading_volume_by_date("20241201", "20241220", "005930")
        print(df.head())

        # 지수 OHLCV
        print("\n[5] KOSPI 지수 (2024-12-01 ~ 2024-12-20)")
        df = client.get_index_ohlcv("20241201", "20241220", "1001")
        print(df.head())

        print("\n" + "=" * 60)
        print("모든 테스트 완료!")
        print("=" * 60)

    except KRX2FARequiredError as e:
        print(f"\n[ERROR] {e}")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_client()
