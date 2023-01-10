# Convenient auxiliary multi-platform module for colored output to the console
# This module has platform agnostic implementation
# Use it as the following:

#   from daffi.utils import colors
#   print(colors.red('abc'))
#   print(colors.intense_blue('xyz'))

# All available functions of this module:
#  - grey
#  - red
#  - green
#  - yellow
#  - blue
#  - magenta
#  - cyan
#  - white
#  - intense_grey
#  - intense_red
#  - intense_green
#  - intense_yellow
#  - intense_blue
#  - intense_magenta
#  - intense_cyan
#  - intense_white

import sys

NAMES = ["grey", "red", "green", "yellow", "blue", "magenta", "cyan", "white"]


def get_pairs():
    for i, name in enumerate(NAMES):
        yield (name, str(30 + i))
        yield "intense_" + name, str(30 + i) + ";1"


def ansi(code):
    return f"\033[{code}m"


def ansi_color(code, s):
    return f"{ansi(code)}{s}{ansi(0)}"


def make_color_fn(code):
    return lambda s: ansi_color(code, s)


if sys.platform == "win32":
    import colorama

    colorama.init(strip=False)
for (name, code) in get_pairs():
    globals()[name] = make_color_fn(code)
