/** The bounded, self-contained raster block emitted by RelayHistory capture. */
export function sessionImage(value: unknown): string | null {
  if (typeof value !== "string" || value.length > 1_398_200) return null;
  const match =
    /^!\[Attached image\]\((data:image\/(png|jpeg|gif|webp);base64,([A-Za-z0-9+/]+={0,2}))\)$/.exec(
      value,
    );
  if (!match) return null;
  try {
    const bytes = atob(match[3]!);
    if (bytes.length > 1_048_576) return null;
    const valid =
      match[2] === "png"
        ? bytes.startsWith("\x89PNG\r\n\x1a\n")
        : match[2] === "jpeg"
          ? bytes.startsWith("\xff\xd8\xff")
          : match[2] === "gif"
            ? /^GIF8[79]a/.test(bytes)
            : bytes.startsWith("RIFF") && bytes.slice(8, 12) === "WEBP";
    return valid ? match[1]! : null;
  } catch {
    return null;
  }
}
