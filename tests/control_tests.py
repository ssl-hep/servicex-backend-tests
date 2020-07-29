# This code controls some of the behavior when we test.

import pytest
import os

run_stress_tests = pytest.mark.skipif(True, reason='Heavy stress tests require a lot of resources and time, skipped except when desired.')