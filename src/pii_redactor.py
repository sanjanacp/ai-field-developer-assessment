import re


REDACTED = "[REDACTED]"
LOCAL = r"[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~\-]"
LABEL = r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
EMAIL = re.compile(rf"{LOCAL}+@{LABEL}(?:\.{LABEL})+(?![a-zA-Z0-9-])")
SSN = re.compile(r"(?<![0-9])[0-9]{3}-[0-9]{2}-[0-9]{4}(?![0-9])")
EMAIL_RUN = re.compile(rf"((?:{LOCAL}|@)+)()")
NUMBER_RUN = re.compile(r"([0-9](?:[ -]?[0-9])*)([ -]?)")


def redact_number(value: str) -> str:
    digits = len(value) - value.count(" ") - value.count("-")
    return REDACTED if digits >= 13 else SSN.sub(REDACTED, value)


def redact_email(value: str) -> str:
    return EMAIL.sub(REDACTED, value) if "@" in value else value


class RunRedactor:
    def __init__(self, pattern, limit, replace):
        self.pattern = pattern
        self.limit = limit
        self.replace = replace
        self.pending = ""
        self.masking = False

    def push(self, text: str, final: bool) -> str:
        text = self.pending + text
        self.pending = ""
        output = []
        end = 0
        for match in self.pattern.finditer(text):
            output.append(text[end:match.start()])
            end = match.end()
            value, separator = match.groups()
            open_run = not final and end == len(text)
            oversized = len(value) > self.limit
            if self.masking:
                self.masking = open_run
            elif oversized:
                output.append(REDACTED)
                self.masking = open_run
            elif open_run:
                self.pending = match[0]
                break
            else:
                output.append(self.replace(value))

            if self.masking:
                self.pending = value[-1] + separator
            else:
                output.append(separator)

        output.append(text[end:])
        return "".join(output)


class PiiRedactor:
    def __init__(self):
        self.email = RunRedactor(EMAIL_RUN, 254, redact_email)
        self.number = RunRedactor(NUMBER_RUN, 37, redact_number)

    def push(self, text: str = "", *, final: bool = False) -> str:
        return self.number.push(self.email.push(text, final), final)
