#!/usr/bin/env python3
"""Regression test for the free-text PHI/PII name detector.

A production review_flags.csv audit found that RequestingService (0032,1033) - a
free-text (VR=LO) field with no structural guarantee about its content - had kept
several bare human names un-redacted, flagged LLM_VERIFIED_CLEAN.
Root cause: the original detector prompt reliably reasoned that a
name in a field labeled "RequestingService" was "probably a service name, not
personal" and returned 0 (not PHI) - a field-label anchoring bias.

Follow-up sweeps (~670 names total, mixing real leaked names, well-known public
figures, and randomly-combined ordinary names across many nationalities) found the
same bias at scale: 7% of ordinary names and up to 40% of well-known names were
missed by the original prompt, almost always with reasoning containing "service
name, not personal". This file collects the 89 hardest cases surfaced by those
sweeps - the ones the original prompt missed - as a standing regression list against
the current detector prompt in luwakx/scripts/detector/detector.py.

Requires a reachable LLM endpoint (CLEAN_DESCRIPTORS_LLM_BASE_URL /
CLEAN_DESCRIPTORS_LLM_MODEL / CLEAN_DESCRIPTORS_LLM_API_KEY, same variables the
anonymization pipeline itself uses - see .env.local.example). Skipped automatically
when no endpoint is configured/reachable, or when CLEAN_DESCRIPTORS_LLM_SIMULATE=1.
"""

import importlib.util
import json
import os
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx

# 89 names that the ORIGINAL detector prompt (pre-fix) failed to classify as PHI/PII
# when presented as the value of a RequestingService (0032,1033) tag. Made up of:
#  - 35 ordinary names randomly cross-combined from public first-name/surname lists
#    spanning dozens of nationalities (7% baseline false-negative rate found here)
#  - 54 well-known real public figures (historical figures, politicians, artists,
#    scientists, athletes - up to 40% false-negative rate found in this group,
#    the field-label bias compounding with a "this is public information" bias)
KNOWN_HARD_NAMES = [
    "Adama Bozic", "Albert Einstein", "Anil Nielsen", "Ariel Try", "Avyan Szabo",
    "Ayrton Senna", "Benazir Bhutto", "Celine Dion", "Chinua Achebe", "Coco Chanel",
    "Corazon Aquino", "Dante Gallo", "Deng Xiaoping", "Diego Rivera", "Edvard Munch",
    "Elias Kang", "Emmanuel Macron", "Erling Haaland", "Ernest Hemingway",
    "Ferenc Puskas", "Fernando Pessoa", "Francisca Wagner", "Franz Kafka",
    "Franz Liszt", "Giorgio Armani", "Habib Szabo", "Hamza Gao", "Ho Chi Minh",
    "Hossein Pen", "Imran Khan", "Indira Gandhi", "Ingmar Bergman", "Jean Sibelius",
    "Jomo Kenyatta", "Jose Marti", "Jouri Arakelyan", "Julimar Ty", "Kabir Yildiz",
    "Kemal Fernando", "Kofi Annan", "Lech Walesa", "Lee Kuan Yew", "Leo Tolstoy",
    "Leonardo da Vinci", "Long Hasani", "Ludwig van Beethoven", "Mahatma Gandhi",
    "Mamadou Hasanov", "Mangal Inoue", "Marian Iliev", "Mateo Bruno", "Mia Dith",
    "Milan Kundera", "Monte Garcia", "Moussa Aquino", "Nelson Mandela", "Nida Din",
    "Nikola Tesla", "Nivi Mean", "Nuka Blondal", "Nurul Park", "Orhan Pamuk",
    "Oumar Colombo", "Pablo Bai", "Pele", "Prem Eder", "Prem Goossens",
    "Rabindranath Tagore", "Raul Murray", "Rembrandt van Rijn", "Rose Prishtina",
    "Ryan Biton", "Saoirse Ronan", "Sheikh Petrosyan", "Shinzo Abe", "Sigmund Freud",
    "Simon Bolivar", "Sun Yat-sen", "Taylor Swift", "Trevor Noah", "Vaclav Havel",
    "Victor Hugo", "Vincent van Gogh", "Winston Churchill", "Wole Soyinka",
    "Xi Jinping", "Yehuda Russo", "Yoko Ono", "Yukio Mishima",
]

DETECTOR_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "luwakx", "scripts", "detector", "detector.py",
)


def _load_detector():
    spec = importlib.util.spec_from_file_location("detector_under_test", DETECTOR_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _llm_endpoint_reachable(base_url, http_headers):
    """Check that base_url actually serves the OpenAI-compatible API, not just that
    something answers the socket. A response (even an HTML error page from a proxy
    whose origin is down, e.g. a Cloudflare Tunnel 530) still "succeeds" as an HTTP
    request, so a plain connectivity check without a status/content check would let
    the test proceed and then hang or fail on every real completion call instead of
    skipping cleanly."""
    if not base_url:
        return False
    try:
        resp = httpx.get(
            base_url.rstrip("/") + "/models", headers=http_headers or {}, timeout=10.0
        )
        return resp.status_code == 200
    except Exception:
        return False


class TestPhiNameDetection(unittest.TestCase):
    """Live-LLM regression test: every name in KNOWN_HARD_NAMES must be classified
    as PHI/PII (result "1") when presented as a RequestingService value, using the
    detector prompt currently in luwakx/scripts/detector/detector.py."""

    @classmethod
    def setUpClass(cls):
        if os.environ.get("CLEAN_DESCRIPTORS_LLM_SIMULATE") == "1":
            raise unittest.SkipTest(
                "CLEAN_DESCRIPTORS_LLM_SIMULATE=1: skipping live PHI name-detection regression test"
            )

        cls.base_url = os.environ.get("CLEAN_DESCRIPTORS_LLM_BASE_URL")
        cls.model = os.environ.get("CLEAN_DESCRIPTORS_LLM_MODEL", "openai/gpt-oss-20b")
        # Some openai-sdk versions reject a blank api_key outright even when auth is
        # actually handled by CLEAN_DESCRIPTORS_LLM_HTTP_HEADERS (e.g. a Cloudflare
        # Access-gated endpoint) - fall back to a non-empty placeholder in that case,
        # same as production would need to for those versions.
        cls.api_key = os.environ.get("CLEAN_DESCRIPTORS_LLM_API_KEY") or "not-needed"
        http_headers_raw = os.environ.get("CLEAN_DESCRIPTORS_LLM_HTTP_HEADERS")
        cls.http_headers = json.loads(http_headers_raw) if http_headers_raw else None

        if not _llm_endpoint_reachable(cls.base_url, cls.http_headers):
            raise unittest.SkipTest(
                f"No reachable LLM endpoint at CLEAN_DESCRIPTORS_LLM_BASE_URL={cls.base_url!r}; "
                "skipping live PHI name-detection regression test"
            )

        cls.detector = _load_detector()

    def _classify(self, name, thread_local):
        if not hasattr(thread_local, "client"):
            thread_local.client = self.detector.create_openai_client(
                self.base_url, self.api_key, http_headers=self.http_headers
            )
        tag_desc = f"(0032,1033) RequestingService: {name}"
        result, reasoning = self.detector.detect_phi_or_pii(
            thread_local.client, tag_desc, dev_mode=False, model=self.model
        )
        return name, str(result).strip(), reasoning

    def test_known_hard_names_detected_as_phi(self):
        thread_local = threading.local()
        failures = []

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._classify, name, thread_local): name
                for name in KNOWN_HARD_NAMES
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    _, result, reasoning = future.result()
                except Exception as exc:
                    failures.append(f"{name!r}: request failed ({exc})")
                    continue
                if result != "1":
                    failures.append(f"{name!r}: expected PHI (1), got {result!r} - {reasoning}")

        if failures:
            self.fail(
                f"{len(failures)}/{len(KNOWN_HARD_NAMES)} known-hard names were NOT "
                f"classified as PHI:\n" + "\n".join(sorted(failures))
            )


if __name__ == "__main__":
    unittest.main()
