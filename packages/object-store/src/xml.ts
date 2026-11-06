/**
 * Minimal extraction for the S3 and Azure list responses. Workers have no
 * DOMParser, and these documents are flat and well-formed, so a tag scan is
 * enough. Not a general XML parser.
 */
export function xmlElements(xml: string, tag: string): string[] {
  const out: string[] = [];
  const re = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`, "g");
  for (const m of xml.matchAll(re)) out.push(m[1]!);
  return out;
}

export function xmlText(xml: string, tag: string): string | undefined {
  const [first] = xmlElements(xml, tag);
  return first === undefined ? undefined : decodeEntities(first);
}

export function decodeEntities(s: string): string {
  return s.replace(
    /&(amp|lt|gt|quot|apos|#\d+|#x[0-9a-fA-F]+);/g,
    (_, e: string) => {
      switch (e) {
        case "amp":
          return "&";
        case "lt":
          return "<";
        case "gt":
          return ">";
        case "quot":
          return '"';
        case "apos":
          return "'";
        default:
          return String.fromCodePoint(
            e.startsWith("#x")
              ? Number.parseInt(e.slice(2), 16)
              : Number(e.slice(1)),
          );
      }
    },
  );
}
