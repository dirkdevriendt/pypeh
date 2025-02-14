from __future__ import annotations

import functools
import inspect

from datetime import datetime
from typing import TYPE_CHECKING

from pypeh.core.abc import Command

if TYPE_CHECKING:
    from typing import Callable
