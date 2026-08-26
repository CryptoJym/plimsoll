export function isLoopbackHostname(hostname: string) {
  const normalized = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  return (
    normalized === "localhost" ||
    normalized.endsWith(".localhost") ||
    normalized === "::1" ||
    /^127(?:\.\d{1,3}){3}$/.test(normalized)
  );
}

export function validatedTransportUrl(raw: string, label: string) {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new Error(`${label} must be a valid absolute URL.`);
  }
  if (url.username || url.password) {
    throw new Error(`${label} must not contain embedded credentials.`);
  }
  if (url.protocol === "https:") return url;
  if (url.protocol === "http:" && isLoopbackHostname(url.hostname)) return url;
  throw new Error(`${label} must use HTTPS (HTTP is allowed only for an explicit loopback development URL).`);
}

export function assertNoRedirect(response: Response, label: string, expectedOrigin: string) {
  if (response.redirected || (response.status >= 300 && response.status < 400)) {
    throw new Error(`${label} redirects are rejected.`);
  }
  if (response.url && new URL(response.url).origin !== expectedOrigin) {
    throw new Error(`${label} response escaped its authenticated origin.`);
  }
}
