import os
import re
import json
from collections import defaultdict
from typing import Dict, List

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - environment may not provide pandas
    pd = None  # type: ignore


class JacocoFunctionCoverage:

    def __init__(self, target_path: str):
        self.target_path = target_path

    def _extract_methods_by_file(self, text: str) -> Dict[str, List[str]]:
        """Parse ``source_code`` text into a mapping of file paths to methods."""
        result: Dict[str, List[str]] = defaultdict(list)
        current_file: str | None = None
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            path_match = re.match(r"^//([\w\-./]+\.java)$", line)
            if path_match:
                current_file = path_match.group(1)
                continue
            if current_file:
                method_match = re.match(
                    r"^(public|protected|private|static|\s)+[\w<>, \[\]]+\s+\w+\s*\([^;]*\)",
                    line,
                )
                if method_match:
                    result[current_file].append(line)
        return dict(result)

    def _extract_package_and_class(self, filepath: str) -> tuple[str, str]:
        """Return (package, class) for a Java ``filepath``."""
        if "src/main/java/" not in filepath:
            return "", ""
        relative = filepath.split("src/main/java/")[1]
        parts = relative.split("/")
        class_name = parts[-1].replace(".java", "")
        package_name = ".".join(parts[:-1])
        return package_name, class_name

   
    def _extract_method_html(self, html_path: str, signature_start: str) -> str:
        if not os.path.exists(html_path):
            return ""
        with open(html_path, "r", encoding="utf-8") as fh:
            html_lines = fh.readlines()
        text_lines = [re.sub(r"<[^>]+>", "", line).strip() for line in html_lines]
        target_lines = [ln.strip() for ln in signature_start.split("\n") if ln.strip()]
        if not target_lines:
            return ""
        start_index = -1
        for idx in range(len(text_lines)):
            if target_lines[0] in text_lines[idx]:
                match = True
                for j in range(1, len(target_lines)):
                    if idx + j >= len(text_lines) or target_lines[j] not in text_lines[idx + j]:
                        match = False
                        break
                if match:
                    start_index = idx
                    break
        if start_index == -1:
            return ""
        method_html: List[str] = []
        brace_count = 0
        for line_html, line_text in zip(html_lines[start_index:], text_lines[start_index:]):
            method_html.append(line_html)
            brace_count += line_text.count("{") - line_text.count("}")
            if brace_count == 0 and line_text.endswith("}"):
                break
        return "".join(method_html)

    def _compute_coverages(self, method_html: str) -> tuple[float, float]:
      
        total_lines = (
            method_html.count('class="fc"')
            + method_html.count('class="pc"')
            + method_html.count('class="nc"')
        )
        covered_lines = method_html.count('class="fc"') + method_html.count('class="pc"')
        line_ratio = covered_lines / total_lines if total_lines else 0.0

      
        branch_pattern = re.compile(r'title="(\d+) of (\d+) branches covered"')
        covered_branches = 0
        total_branches = 0
        for covered, total in branch_pattern.findall(method_html):
            covered_branches += int(covered)
            total_branches += int(total)
        branch_ratio = (
            covered_branches / total_branches if total_branches else 0.0
        )

        return line_ratio, branch_ratio

   
    def collect_from_excel(self, excel_path: str, output_path: str) -> dict:

        if pd is None:
            raise RuntimeError("pandas is required to read Excel files")
        df = pd.read_excel(excel_path)
        line_coverages: List[float] = []
        branch_coverages: List[float] = []
        details: List[dict] = []
        for text in df.get("source_code", []).dropna():
            methods_map = self._extract_methods_by_file(str(text))
            for file_path, methods in methods_map.items():
                package, class_name = self._extract_package_and_class(file_path)
                if not package:
                    continue
                html_file = os.path.join(
                    self.target_path,
                    "target",
                    "site",
                    "jacoco",
                    package,
                    f"{class_name}.java.html",
                )
                for method in methods:
                    html = self._extract_method_html(html_file, method)
                    if not html:
                        continue
                    line_ratio, branch_ratio = self._compute_coverages(html)
                    if line_ratio == 0.0 and branch_ratio == 0.0:
                        continue
                    line_coverages.append(line_ratio)
                    branch_coverages.append(branch_ratio)
                    details.append({
                        "file": file_path,
                        "method": method,
                        "line_coverage": line_ratio,
                        "branch_coverage": branch_ratio,
                    })
        avg_line = sum(line_coverages) / len(line_coverages) if line_coverages else 0.0
        avg_branch = sum(branch_coverages) / len(branch_coverages) if branch_coverages else 0.0
        result = {
            "average_line_coverage": avg_line,
            "average_branch_coverage": avg_branch,
            "details": details,
        }
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(result, fh, indent=2)
        return result

if __name__ == "__main__":
   
    repo_name = "hbase"


    file_path = {
        "hbase" : "",
        "hdfs" : ""
    }
    output_path = {
        "hbase" : "hbase.json",
        "hdfs" : "hdfs.json"
    }
    excel_path = {
        "hbase" : "hbase.xlsx",
        "hdfs" : "hdfs.xlsx"
    }


    collector = JacocoFunctionCoverage(file_path[repo_name])
    summary = collector.collect_from_excel(excel_path[repo_name], output_path[repo_name])
    print(json.dumps(summary, indent=2))