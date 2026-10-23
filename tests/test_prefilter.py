"""Test bộ lọc token trước khi chạy (`web/prefilter.py`) — không cần mạng.

Trọng tâm là hàm `classify_payload` (quyết định giữ/loại acc) và phần đọc JWT
(email, hạn, account_id, lỗi định dạng) vì đó là chỗ dễ sai âm thầm nhất.
"""
from __future__ import annotations

import base64
import json
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from web import prefilter  # noqa: E402


def make_token(email: str = "a@b.c", exp: float | None = None, account_id: str = "acc_1") -> str:
    def b64(data: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()

    claims = {
        "https://api.openai.com/profile": {"email": email},
        "https://api.openai.com/auth": {"chatgpt_account_id": account_id},
        "exp": int(exp if exp is not None else time.time() + 3600),
    }
    return f"{b64({'alg': 'RS256'})}.{b64(claims)}.sig"


def payload(*, deactivated: bool = False, plan: str = "free", sub: bool = False,
            campaigns: dict | None = None) -> dict:
    return {
        "accounts": {
            "acc_1": {
                "account": {"is_deactivated": deactivated, "plan_type": plan},
                "entitlement": {"has_active_subscription": sub},
                "eligible_promo_campaigns": campaigns if campaigns is not None else {
                    "plus-1-month-free": {"id": "plus-1-month-free"},
                },
            }
        }
    }


class ClassifyPayloadTests(unittest.TestCase):
    def test_free_account_with_campaign_is_eligible(self):
        verdict, detail, extra = prefilter.classify_payload(payload(), "plus-1-month-free")
        self.assertEqual(verdict, "eligible")
        self.assertEqual(extra["campaigns"], ["plus-1-month-free"])
        self.assertIn("plus-1-month-free", detail)

    def test_no_campaigns_is_not_eligible(self):
        verdict, _, _ = prefilter.classify_payload(payload(campaigns={}), "plus-1-month-free")
        self.assertEqual(verdict, "not_eligible")

    def test_other_campaign_is_not_eligible_for_our_promo(self):
        data = payload(campaigns={"trial-7d": {"id": "trial-7d"}})
        verdict, detail, _ = prefilter.classify_payload(data, "plus-1-month-free")
        self.assertEqual(verdict, "not_eligible")
        self.assertIn("trial-7d", detail)

    def test_other_campaign_is_eligible_when_promo_matches(self):
        data = payload(campaigns={"trial-7d": {"id": "trial-7d"}})
        self.assertEqual(prefilter.classify_payload(data, "trial-7d")[0], "eligible")

    def test_plus_plan_is_dropped(self):
        self.assertEqual(prefilter.classify_payload(payload(plan="plus"), "x")[0], "plus_active")

    def test_active_subscription_is_dropped_even_if_plan_free(self):
        self.assertEqual(prefilter.classify_payload(payload(sub=True), "x")[0], "active_sub")

    def test_deactivated_is_banned(self):
        self.assertEqual(prefilter.classify_payload(payload(deactivated=True), "x")[0], "banned")

    def test_banned_wins_over_plus(self):
        data = payload(deactivated=True, plan="plus")
        self.assertEqual(prefilter.classify_payload(data, "x")[0], "banned")

    def test_empty_payload_is_network_error(self):
        self.assertEqual(prefilter.classify_payload({}, "x")[0], "network_error")
        self.assertEqual(prefilter.classify_payload({"accounts": {}}, "x")[0], "network_error")


class TokenParsingTests(unittest.TestCase):
    def test_email_from_profile_claim(self):
        self.assertEqual(prefilter.token_email(make_token("me@example.com")), "me@example.com")

    def test_email_empty_on_garbage(self):
        self.assertEqual(prefilter.token_email("not-a-jwt"), "")

    def test_expired_token_flagged_without_network(self):
        token = make_token(exp=time.time() - 10)
        result = prefilter.probe_token(token)
        self.assertEqual(result.verdict, "expired")
        self.assertEqual(result.ms, 0)

    def test_malformed_token_flagged_without_network(self):
        self.assertEqual(prefilter.probe_token("abc.def").verdict, "bad_token")

    def test_401_body_token_expired_maps_to_token_expired(self):
        class FakeResp:
            status_code = 401

            @staticmethod
            def json():
                return {"error": {"code": "token_expired",
                                  "message": "Your authentication token has expired."}}

        result = prefilter._result_from_response(FakeResp(), "fp", "a@b.c", "plus-1-month-free", 0.0)
        self.assertEqual(result.verdict, "token_expired")
        self.assertIn("token_expired", result.detail)
        self.assertIn("token_expired", prefilter.DROP_VERDICTS)

    def test_401_without_code_stays_token_invalid(self):
        class FakeResp:
            status_code = 401
            text = "unauthorized"

            @staticmethod
            def json():
                raise ValueError("not json")

        result = prefilter._result_from_response(FakeResp(), "fp", "", "x", 0.0)
        self.assertEqual(result.verdict, "token_invalid")

    def test_fingerprint_is_stable_and_short(self):
        token = make_token()
        self.assertEqual(prefilter.fingerprint(token), prefilter.fingerprint(token))
        self.assertEqual(len(prefilter.fingerprint(token)), 10)

    def test_headers_carry_account_id(self):
        headers = prefilter._headers(make_token(account_id="acc_42"))
        self.assertEqual(headers["ChatGPT-Account-ID"], "acc_42")
        self.assertTrue(headers["Authorization"].startswith("Bearer "))


class VerdictBookkeepingTests(unittest.TestCase):
    def test_drop_verdicts_are_exactly_the_dead_ones(self):
        self.assertEqual(
            prefilter.DROP_VERDICTS,
            {"plus_active", "active_sub", "banned", "token_invalid", "token_expired",
             "bad_token", "expired"},
        )

    def test_not_eligible_is_kept(self):
        """not_eligible/network_error KHÔNG bị loại: để pipeline tự quyết (có thể retry proxy)."""
        self.assertNotIn("not_eligible", prefilter.DROP_VERDICTS)
        self.assertNotIn("network_error", prefilter.DROP_VERDICTS)

    def test_pick_proxy_round_robin(self):
        pool = ["http://a", "http://b"]
        self.assertEqual(prefilter.pick_proxy(pool, 0), "http://a")
        self.assertEqual(prefilter.pick_proxy(pool, 3), "http://b")
        self.assertEqual(prefilter.pick_proxy([], 5), "")

    def test_eligible_tokens_maps_fingerprints_back(self):
        token = make_token()
        run_id = prefilter.start([token], [], workers=1)
        for _ in range(50):
            snap = prefilter.status(run_id) or {}
            if snap.get("state") != "running":
                break
            time.sleep(0.02)
        # token giả -> 401 hoặc network_error, nhưng hàm map phải chạy được và trả list
        picked = prefilter.eligible_tokens(run_id, [token])
        self.assertIn(picked, ([], [token]))


if __name__ == "__main__":
    unittest.main()
