from enum import Enum
import re


RESET = "\033[0m"
BOLD = "\033[1m"
UNDERLINE = "\033[4m"
INVERT = "\033[7m"

BLACK = "\033[30m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"
WHITE = "\033[37m"

# Grayscale colors (256-color mode)
DARK_GRAY = "\033[38;5;238m"
MEDIUM_GRAY = "\033[38;5;244m"
LIGHT_GRAY = "\033[38;5;250m"
