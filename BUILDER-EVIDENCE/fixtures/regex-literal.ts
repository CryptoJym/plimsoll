/**
 * REVIEW-68-r2 N3 counterexample. Every line of code below is real code; the
 * only comments are this block and the two marked `// comment`. A correct
 * stripper removes exactly those three and keeps every statement.
 *
 * `strip-comments-naive.mjs` does not: at `\/\/` inside the first regex it
 * enters line-comment state and swallows the rest of that line, and at `/[/*]/`
 * it enters block-comment state and swallows everything up to the next `*` `/`
 * — several statements later. Its digest is then a digest of a file that was
 * never written. (The second regex, `/\/\*[\s\S]*?\*\//`, only hits the line-
 * comment path: its closing `\/` + delimiter is `//`, not a raw `/*`.)
 */
export const httpsPrefix = /^https:\/\/example\.test\//;
export const blockCommentShape = /\/\*[\s\S]*?\*\//; // comment
export const stillCode = "this line survives a correct stripper";
export function readsLikeCode(url: string) {
  return httpsPrefix.test(url) && !blockCommentShape.test(url); // comment
}
export const slashStarInCharClass = /[/*]/;
export const swallowedByNaiveBlock = "naive block-comment state eats this";
export const alsoSwallowed = "and this";
export const blockCloser = "*/";
