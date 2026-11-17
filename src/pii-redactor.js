const email = /\b[A-Z0-9.!#$%&'*+\-/=?^_`{|}~]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+\b/gi;
const ssn = /\b\d{3}-\d{2}-\d{4}\b/g;
const card = /(?<!\d)(?:\d[ -]?){13,19}(?!\d)/g;
const emailChars = /[A-Z0-9.!#$%&'*+\-/=?^_`{|}~@-]/i;
const numberChars = /[\d -]/;

export class PiiRedactor {
  #pending = "";

  push(text, final = false) {
    this.#pending += text;
    let value = this.#pending
      .replace(email, "[REDACTED]")
      .replace(ssn, "[REDACTED]")
      .replace(card, "[REDACTED]");

    if (final) {
      this.#pending = "";
      return value;
    }

    let safeCut = value.length;
    let emailStart = value.length;
    while (emailStart > 0 && emailChars.test(value[emailStart - 1])) emailStart--;
    if (value.length - emailStart <= 254) safeCut = Math.min(safeCut, emailStart);
    else safeCut = Math.min(safeCut, value.length - 254);

    let numberStart = value.length;
    while (numberStart > 0 && numberChars.test(value[numberStart - 1])) numberStart--;
    safeCut = Math.min(safeCut, Math.max(numberStart, value.length - 24));

    this.#pending = value.slice(safeCut);
    return value.slice(0, safeCut);
  }
}
