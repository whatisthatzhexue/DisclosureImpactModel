#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os
import json
import hashlib
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any, Set, Union, Iterator
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from enum import Enum
import logging
from functools import lru_cache
import contextlib

MAX_CHUNK_SIZE: int = 31000
MAX_CONTENT_SIZE: int = MAX_CHUNK_SIZE - 1000
MIN_TABLE_LINES: int = 3
TABLE_SCORE_THRESHOLD: int = 6

PROTECTED_KEYWORDS: Dict[str, int] = {
    'revenue': 3, 'profit': 3, 'loss': 3, 'income': 3, 'asset': 3,
    'liability': 3, 'equity': 3, 'dividend': 2, 'earnings': 3,
    'pbt': 3, 'pat': 3, 'financial': 2, 'million': 2, 'billion': 2,
    'rm': 2, 'marine': 1, 'livestock': 1, 'poultry': 1, 'palm oil': 2,
    'surimi': 1, 'fishmeal': 1, 'aquaculture': 1, 'sustainability': 1,
    'esg': 1, 'emission': 1, 'carbon': 1, 'board': 1, 'director': 1
}

HEADER_FOOTER_PATTERNS: List[str] = [
    r'^\d+$', r'^Page \d+$', r'^\d+\s+of\s+\d+$', r'^\d+\s*/\s*\d+$',
    r'^- \d+ -$', r'\[\s*\d+\s*\]', r'^\s*$',
    r'^POWER ROOT BERHAD$', r'^Power Root Berhad$',
    r'^Registration Number: 200601013517 \(733268-U\)$',
    r'^QL RESOURCES BERHAD$', r'^QL Resources Berhad$', r'^\(428915-X\)$',
    r'^INTEGRATED ANNUAL REPORT 20\d{2}$', r'^IAR 20\d{2}$',
    r'^BERJAYA FOOD BERHAD$', r'^BERJAYA FOOD BERHAD \(876057-U\)$',
    r'^ANNUAL REPORT 20\d{2}$', r'^\d+\s+BERJAYA FOOD BERHAD.*$',
    r'^Fraser & Neave Holdings Bhd$', r'^Annual Report 20\d{2}$',
    r'^Communications, Corporate Affairs and Sustainability Department$',
    r'^No\. 1 Jalan Bukit Belimbing 26/38.*$', r'^groupcomms@fn\.com\.my$',
    r'^www\.(powerroot|ql|berjaya|fn)\.com$',
    r'^www\.(powerroot|ql|berjaya|fn)\.com\.my$',
    r'^BFood_AR\d+_update\d+\.qxp_Layout \d+.*$',
    r'^QL AR\d+\.qxp_Layout \d+$', r'^[PQBF]\d+\.txt$'
]

SECTION_KEYWORDS: List[str] = [
    "CORPORATE PROFILE", "CHAIRMAN'S STATEMENT", "EXECUTIVE CHAIRMAN'S STATEMENT",
    "MANAGEMENT DISCUSSION & ANALYSIS", "MANAGEMENT DISCUSSION AND ANALYSIS", "MD&A",
    "FINANCIAL RESULTS", "DIVIDEND", "CORPORATE SOCIAL RESPONSIBILITY", "CSR",
    "SUSTAINABILITY STATEMENT", "CORPORATE GOVERNANCE",
    "STATEMENT ON CORPORATE GOVERNANCE", "RISK MANAGEMENT",
    "STATEMENT ON RISK MANAGEMENT AND INTERNAL CONTROL", "AUDIT COMMITTEE REPORT",
    "AUDIT AND RISK MANAGEMENT COMMITTEE REPORT", "FINANCIAL STATEMENTS",
    "NOTES TO THE FINANCIAL STATEMENTS", "DIRECTORS' REPORT", "STATEMENT BY DIRECTORS",
    "STATUTORY DECLARATION", "INDEPENDENT AUDITORS' REPORT", "CORPORATE STRUCTURE",
    "GROUP FINANCIAL SUMMARY", "GROUP FINANCIAL HIGHLIGHTS", "FIVE-YEAR FINANCIAL SUMMARY",
    "LIST OF PROPERTIES", "ANALYSIS OF SHAREHOLDINGS", "NOTICE OF ANNUAL GENERAL MEETING",
    "FORM OF PROXY"
]

FINANCIAL_TABLE_HEADERS: List[str] = [
    'INCOME STATEMENT', 'STATEMENT OF PROFIT OR LOSS', 'STATEMENT OF COMPREHENSIVE INCOME',
    'BALANCE SHEET', 'STATEMENT OF FINANCIAL POSITION', 'CASH FLOW STATEMENT',
    'STATEMENT OF CASH FLOWS', 'STATEMENT OF CHANGES IN EQUITY',
    'NOTES TO THE FINANCIAL STATEMENTS', 'REVENUE', 'PROFIT', 'ASSETS', 'LIABILITIES',
    '损益表', '资产负债表', '现金流量表', '财务状况表'
]

PAGE_PATTERNS: List[Tuple[re.Pattern, float]] = [
    (re.compile(r'^\s*\d+\s*$'), 1.0),
    (re.compile(r'^\s*[-–—]\s*\d+\s*[-–—]\s*$'), 0.95),
    (re.compile(r'^\s*\[\s*\d+\s*\]\s*$'), 0.95),
    (re.compile(r'^\s*Page\s+\d+\s*$', re.IGNORECASE), 0.95),
    (re.compile(r'^\s*PAGE\s+\d+\s*$', re.IGNORECASE), 0.95),
    (re.compile(r'^\s*Pg\.?\s*\d+\s*$', re.IGNORECASE), 0.9),
    (re.compile(r'^\s*第\s*\d+\s*页\s*$'), 0.95),
    (re.compile(r'^\s*\d+\s*/\s*\d+\s*$'), 0.8),
    (re.compile(r'^\s*\d+\s*of\s*\d+\s*$', re.IGNORECASE), 0.85),
    (re.compile(r'^\s*-\s*\d+\s*-\s*$'), 0.9)
]

COMPILED_HEADER: List[re.Pattern] = [re.compile(p, re.IGNORECASE) for p in HEADER_FOOTER_PATTERNS]
COMPILED_SECTION: List[re.Pattern] = [re.compile(rf'^\s*{re.escape(k)}\s*$', re.IGNORECASE) for k in SECTION_KEYWORDS]
COMPILED_FINANCIAL_HEADERS: List[re.Pattern] = [re.compile(rf'.*{re.escape(h)}.*', re.IGNORECASE) for h in
                                                FINANCIAL_TABLE_HEADERS]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger: logging.Logger = logging.getLogger(__name__)


@contextlib.contextmanager
def open_file(path: str, mode: str = 'r', encoding: str = 'utf-8') -> Iterator[Any]:
    """Context manager for file operations with error handling"""
    f = None
    try:
        f = open(path, mode, encoding=encoding)
        yield f
    except Exception as e:
        logger.error(f"File operation failed: {path} - {e}")
        raise
    finally:
        if f:
            f.close()


@contextlib.contextmanager
def read_file(path: str) -> Iterator[str]:
    """Context manager for reading files"""
    with open_file(path, 'r') as f:
        yield f.read()


@contextlib.contextmanager
def write_file(path: str) -> Iterator[Any]:
    """Context manager for writing files"""
    with open_file(path, 'w') as f:
        yield f


@dataclass
class ExtractionResult:
    value: float
    original: str
    context: str
    line_num: int
    is_negative: bool
    unit: Optional[str] = None
    confidence: float = 1.0


@dataclass
class ChunkInfo:
    id: int
    text: str
    tokens: int
    sections: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)


class ValidationLevel(Enum):
    STRICT = "strict"
    NORMAL = "normal"
    LENIENT = "lenient"


def init_tokenizer() -> Tuple[Optional[Any], bool]:
    try:
        import tiktoken
        return tiktoken.get_encoding('cl100k_base'), True
    except ImportError:
        return None, False


class OutlierDetector:
    def __init__(self, z_score_threshold: float = 3.5, iqr_multiplier: float = 2.0) -> None:
        self.z_score_threshold: float = z_score_threshold
        self.iqr_multiplier: float = iqr_multiplier

    def detect_outliers(self, numbers: List[Dict], level: ValidationLevel = ValidationLevel.NORMAL) -> List[Dict]:
        if len(numbers) < 15:
            return []

        df: pd.DataFrame = pd.DataFrame(numbers)

        if 'context' in df.columns and len(df['context'].unique()) > 1:
            outliers: List[Dict] = []
            for context, group in df.groupby('context'):
                if len(group) >= 5:
                    group_outliers: List[Dict] = self._detect_group_outliers(group, level)
                    outliers.extend(group_outliers)
            return outliers

        return self._detect_group_outliers(df, level)

    def _detect_group_outliers(self, group: pd.DataFrame, level: ValidationLevel) -> List[Dict]:
        values: np.ndarray = group['value'].values
        if len(values) < 5:
            return []

        thresholds: Dict = {
            ValidationLevel.STRICT: {'z': 3.0, 'iqr': 1.5, 'jump': 4.0},
            ValidationLevel.NORMAL: {'z': 3.5, 'iqr': 2.0, 'jump': 5.0},
            ValidationLevel.LENIENT: {'z': 4.0, 'iqr': 2.5, 'jump': 6.0}
        }
        t: Dict = thresholds[level]

        outliers: List[Dict] = []
        mean: float = float(np.mean(values))
        std: float = float(np.std(values))

        if std > 0:
            z_scores: np.ndarray = np.abs((values - mean) / std)
            z_outliers: List[int] = group[z_scores > t['z']].index.tolist()
            for idx in z_outliers:
                record: Dict = group.loc[idx].to_dict() if hasattr(group.loc[idx], 'to_dict') else dict(group.loc[idx])
                outliers.append({**record, 'type': 'z_score', 'score': float(z_scores[idx])})

        Q1: float = float(np.percentile(values, 25))
        Q3: float = float(np.percentile(values, 75))
        IQR: float = Q3 - Q1

        if IQR > 0:
            iqr_outliers: List[int] = group[
                (values < Q1 - t['iqr'] * IQR) | (values > Q3 + t['iqr'] * IQR)].index.tolist()
            for idx in iqr_outliers:
                if idx not in z_outliers:
                    record = group.loc[idx].to_dict() if hasattr(group.loc[idx], 'to_dict') else dict(group.loc[idx])
                    outliers.append({**record, 'type': 'iqr'})

        for i in range(1, len(group)):
            prev: float = float(values[i - 1])
            curr: float = float(values[i])
            if abs(curr) > 0 and abs(prev) > 0:
                ratio: float = curr / prev
                if ratio > t['jump'] or ratio < 1 / t['jump']:
                    record = group.iloc[i].to_dict() if hasattr(group.iloc[i], 'to_dict') else dict(group.iloc[i])
                    outliers.append({**record, 'type': 'jump'})

        return outliers

    def get_outlier_report(self, numbers: List[Dict], level: ValidationLevel = ValidationLevel.NORMAL) -> Dict:
        outliers: List[Dict] = self.detect_outliers(numbers, level)

        if not outliers:
            return {'has_outliers': False, 'outliers': []}

        types: List[str] = list(set(o.get('type', 'unknown') for o in outliers))

        return {
            'has_outliers': True,
            'total': len(outliers),
            'types': types,
            'outliers': outliers[:20],
            'summary': {
                'z_score': sum(1 for o in outliers if o.get('type') == 'z_score'),
                'iqr': sum(1 for o in outliers if o.get('type') == 'iqr'),
                'jump': sum(1 for o in outliers if o.get('type') == 'jump')
            }
        }


class DataDeduplicator:
    def __init__(self, similarity_threshold: float = 0.9) -> None:
        self.similarity_threshold: float = similarity_threshold

    def compute_hash(self, text: str) -> str:
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    def compute_similarity(self, text1: str, text2: str) -> float:
        set1: Set[str] = set(text1.lower().split())
        set2: Set[str] = set(text2.lower().split())

        if not set1 or not set2:
            return 0.0

        intersection: int = len(set1.intersection(set2))
        union: int = len(set1.union(set2))

        return intersection / union if union > 0 else 0.0

    def deduplicate_texts(self, texts: List[str]) -> List[str]:
        if len(texts) <= 1:
            return texts

        seen_hashes: Dict[str, str] = {}
        unique_texts: List[str] = []

        for text in texts:
            h: str = self.compute_hash(text)
            if h not in seen_hashes:
                seen_hashes[h] = text
                unique_texts.append(text)

        i: int = 0
        while i < len(unique_texts):
            j: int = i + 1
            while j < len(unique_texts):
                sim: float = self.compute_similarity(unique_texts[i], unique_texts[j])
                if sim > self.similarity_threshold:
                    if len(unique_texts[i]) >= len(unique_texts[j]):
                        unique_texts.pop(j)
                    else:
                        unique_texts.pop(i)
                        i -= 1
                        break
                else:
                    j += 1
            i += 1

        return unique_texts

    def deduplicate_numbers(self, numbers: List[Dict]) -> List[Dict]:
        if len(numbers) <= 1:
            return numbers

        seen: Set[Tuple] = set()
        unique: List[Dict] = []

        sorted_numbers: List[Dict] = sorted(numbers, key=lambda x: abs(x.get('value', 0)), reverse=True)

        for n in sorted_numbers:
            key: Tuple = (round(n.get('value', 0), 2), n.get('context', '')[:50])
            if key not in seen:
                seen.add(key)
                unique.append(n)

        return unique

    def merge_duplicate_sections(self, sections: Dict[str, str]) -> Dict[str, str]:
        merged: Dict[str, str] = {}
        items: List[Tuple[str, str]] = [(name, text) for name, text in sections.items()]

        i: int = 0
        while i < len(items):
            name1: str
            text1: str
            name1, text1 = items[i]
            if name1 in merged:
                i += 1
                continue

            similar: List[str] = [name1]
            for j in range(i + 1, len(items)):
                name2: str
                text2: str
                name2, text2 = items[j]
                sim: float = self.compute_similarity(text1, text2)
                if sim > self.similarity_threshold:
                    similar.append(name2)

            if len(similar) > 1:
                merged_name: str = ' + '.join(similar)
                merged[merged_name] = text1
            else:
                merged[name1] = text1

            i += 1

        return merged


class FinancialDataExtractor:
    def __init__(self) -> None:
        self.units: Dict[str, int] = {
            'million': 1_000_000, 'billion': 1_000_000_000,
            'thousand': 1_000, '千': 1_000, '万': 10_000, '亿': 100_000_000
        }
        self.currency_patterns: List[str] = [
            r'rm\s*([\d\.,]+)', r'rm([\d\.,]+)', r'\$\s*([\d\.,]+)',
            r'([\d\.,]+)\s*million', r'([\d\.,]+)\s*billion'
        ]

    def extract_numbers(self, text: str, min_value: float = 1.0) -> List[Dict]:
        results: List[Dict] = []
        lines: List[str] = text.split('\n')

        for i, line in enumerate(lines):
            for match in re.finditer(r'\(\s*([\d\.,]+)\s*\)', line):
                try:
                    num_str: str = match.group(1).replace(',', '')
                    num: float = float(num_str)

                    if abs(num) >= min_value:
                        context: str = line[max(0, match.start() - 30):min(len(line), match.end() + 30)].strip()

                        results.append({
                            'value': -num,
                            'original': f'({match.group(1)})',
                            'context': context,
                            'line_num': i,
                            'is_negative': True,
                            'confidence': 0.95
                        })
                except ValueError:
                    continue

            for match in re.finditer(r'(?<![\(\d,])(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+\.\d+)(?![\)\d,])', line):
                if line[max(0, match.start() - 1):match.start()] == '(':
                    continue

                try:
                    num_str = match.group(1).replace(',', '')
                    value: float = float(num_str)

                    unit_mult: int = 1
                    unit_found: Optional[str] = None

                    for unit, mult in self.units.items():
                        if unit in line[match.end():match.end() + 20].lower():
                            unit_mult = mult
                            unit_found = unit
                            break

                    final_value: float = value * unit_mult

                    if abs(final_value) >= min_value:
                        context = line[max(0, match.start() - 30):min(len(line), match.end() + 30)].strip()

                        results.append({
                            'value': final_value,
                            'original': match.group(1),
                            'unit': unit_found,
                            'context': context,
                            'line_num': i,
                            'is_negative': False,
                            'confidence': 0.9 if unit_found else 0.8
                        })
                except ValueError:
                    continue

        return results

    def extract_financial_statements(self, text: str) -> Dict[str, pd.DataFrame]:
        statements: Dict[str, pd.DataFrame] = {}
        sections: Dict[str, str] = self._identify_financial_sections(text)

        for name, section_text in sections.items():
            df: pd.DataFrame = self._parse_financial_table(section_text)
            if not df.empty:
                statements[name] = df

        return statements

    def _identify_financial_sections(self, text: str) -> Dict[str, str]:
        sections: Dict[str, str] = {}
        lines: List[str] = text.split('\n')

        current: Optional[str] = None
        content: List[str] = []

        for line in lines:
            is_header: bool = False
            for header in FINANCIAL_TABLE_HEADERS:
                if re.search(rf'^\s*{re.escape(header)}\s*$', line, re.IGNORECASE):
                    if current and content:
                        sections[current] = '\n'.join(content)
                    current = header
                    content = [line]
                    is_header = True
                    break

            if not is_header and current:
                content.append(line)

        if current and content:
            sections[current] = '\n'.join(content)

        return sections

    def _parse_financial_table(self, table_text: str) -> pd.DataFrame:
        lines: List[str] = table_text.split('\n')
        data: List[Dict] = []

        for line in lines:
            parts: List[str] = re.split(r'\s{2,}', line.strip())

            if len(parts) >= 2:
                item: str = parts[0]
                numbers: List[Optional[float]] = []

                for p in parts[1:]:
                    num: Optional[float] = self._clean_number(p)
                    numbers.append(num)

                if any(n is not None for n in numbers):
                    data.append({'item': item, 'values': numbers})

        if not data:
            return pd.DataFrame()

        max_len: int = max(len(d['values']) for d in data)

        df_data: Dict[str, List] = {}
        for d in data:
            values: List = d['values'] + [None] * (max_len - len(d['values']))
            df_data[d['item']] = values

        return pd.DataFrame(df_data)

    def _clean_number(self, num_str: str) -> Optional[float]:
        num_str = num_str.strip()

        multiplier: int = 1
        if re.match(r'\(\s*[\d\.,]+\s*\)', num_str):
            num_str = num_str.replace('(', '').replace(')', '')
            multiplier = -1

        match: Optional[re.Match] = re.search(r'[\d\.,]+', num_str)
        if not match:
            return None

        num_str = match.group().replace(',', '')

        try:
            return float(num_str) * multiplier
        except ValueError:
            return None


class DataValidator:
    def __init__(self, level: ValidationLevel = ValidationLevel.NORMAL) -> None:
        self.level: ValidationLevel = level
        self.outlier_detector: OutlierDetector = OutlierDetector()
        self.deduplicator: DataDeduplicator = DataDeduplicator()

    def validate(self, original: str, cleaned: str, numbers: List[Dict]) -> Dict:
        report: Dict = {
            'timestamp': datetime.now().isoformat(),
            'level': self.level.value,
            'stats': {
                'original_chars': len(original),
                'cleaned_chars': len(cleaned),
                'reduction_pct': round((len(original) - len(cleaned)) / len(original) * 100, 2),
                'original_lines': len(original.split('\n')),
                'cleaned_lines': len(cleaned.split('\n'))
            },
            'checks': {},
            'warnings': [],
            'errors': []
        }

        required_terms: List[str] = ['revenue', 'profit', 'asset', 'liability', 'equity']
        missing: List[str] = [t for t in required_terms if t not in cleaned.lower()]

        for term in required_terms:
            report['checks'][f'has_{term}'] = term in cleaned.lower()

        if missing:
            report['warnings'].append(f'Missing terms: {missing}')

        years: List[str] = re.findall(r'20\d{2}|19\d{2}', cleaned)
        report['checks']['years_found'] = len(years) > 0
        report['years'] = list(set(years))[:10]

        if not years:
            report['warnings'].append('No years found')

        neg_count: int = len(re.findall(r'-\d+', cleaned))
        orig_parentheses: int = len(re.findall(r'\(\s*[\d\.,]+\s*\)', original))

        report['checks']['negative_conversion'] = neg_count > 0 or orig_parentheses == 0

        if orig_parentheses > 0 and neg_count == 0:
            report['errors'].append('No negatives converted from parentheses')

        if numbers:
            values: List[float] = [n['value'] for n in numbers]

            report['numbers'] = {
                'total': len(values),
                'min': float(min(values)) if values else 0,
                'max': float(max(values)) if values else 0,
                'mean': float(sum(values) / len(values)) if values else 0,
                'negatives': sum(1 for n in numbers if n.get('is_negative', False)),
                'positives': sum(1 for n in numbers if not n.get('is_negative', True)),
                'zeros': sum(1 for n in numbers if n['value'] == 0)
            }

            outlier_report: Dict = self.outlier_detector.get_outlier_report(numbers, self.level)
            if outlier_report['has_outliers']:
                report['outliers'] = outlier_report

        unique_numbers: List[Dict] = self.deduplicator.deduplicate_numbers(numbers)
        report['duplicates'] = {
            'original': len(numbers),
            'unique': len(unique_numbers),
            'removed': len(numbers) - len(unique_numbers)
        }

        table_lines: int = sum(1 for line in cleaned.split('\n') if self._is_table_line(line))
        report['tables'] = {
            'detected_lines': table_lines,
            'percentage': round(table_lines / max(1, len(cleaned.split('\n'))) * 100, 2)
        }

        return report

    def _is_table_line(self, line: str) -> bool:
        stripped: str = line.strip()
        if not stripped or len(stripped) < 15:
            return False

        parts: List[str] = re.split(r'\s{3,}', stripped)
        if len(parts) >= 3:
            numbers: List[str] = re.findall(r'[\d\.,]+', stripped)
            if len(numbers) >= 2:
                return True

        return False

    def generate_report_text(self, report: Dict) -> str:
        lines: List[str] = []
        lines.append("=" * 60)
        lines.append("DATA CLEANING QUALITY REPORT")
        lines.append(f"Generated: {report['timestamp']}")
        lines.append(f"Level: {report['level']}")
        lines.append("=" * 60)

        lines.append(
            f"Original: {report['stats']['original_chars']:,} chars, {report['stats']['original_lines']} lines")
        lines.append(f"Cleaned: {report['stats']['cleaned_chars']:,} chars, {report['stats']['cleaned_lines']} lines")
        lines.append(f"Reduction: {report['stats']['reduction_pct']}%")
        lines.append("-" * 40)

        lines.append("CHECKS:")
        for check, passed in report['checks'].items():
            status: str = "✓" if passed else "✗"
            lines.append(f"  {status} {check}")
        lines.append("-" * 40)

        if report['warnings']:
            lines.append("WARNINGS:")
            for w in report['warnings']:
                lines.append(f"  ⚠ {w}")

        if report['errors']:
            lines.append("ERRORS:")
            for e in report['errors']:
                lines.append(f"  ✘ {e}")

        if 'numbers' in report:
            lines.append("-" * 40)
            lines.append("NUMBERS:")
            for k, v in report['numbers'].items():
                if isinstance(v, float):
                    lines.append(f"  {k}: {v:,.2f}")
                else:
                    lines.append(f"  {k}: {v}")

        if 'outliers' in report and report['outliers']['has_outliers']:
            lines.append("-" * 40)
            lines.append(f"OUTLIERS: {report['outliers']['total']}")
            lines.append(f"  Types: {', '.join(report['outliers']['types'])}")

        if 'duplicates' in report:
            lines.append("-" * 40)
            lines.append("DUPLICATES:")
            lines.append(f"  Removed: {report['duplicates']['removed']}")

        lines.append("=" * 60)

        return '\n'.join(lines)


class AnnualReportCleaner:
    def __init__(self, level: ValidationLevel = ValidationLevel.NORMAL) -> None:
        self.level: ValidationLevel = level
        self.stats: Dict[str, int] = {
            'removed_lines': 0,
            'protected_tables': 0,
            'converted_negatives': 0,
            'removed_pages': 0,
            'removed_headers': 0
        }

        self.extractor: FinancialDataExtractor = FinancialDataExtractor()
        self.validator: DataValidator = DataValidator(level)
        self.deduplicator: DataDeduplicator = DataDeduplicator()

        self.thresholds: Dict = {
            ValidationLevel.STRICT: {
                'page_weight': 0.9,
                'table_score': 5,
                'min_table_lines': 2,
                'header_freq': 4
            },
            ValidationLevel.NORMAL: {
                'page_weight': 0.8,
                'table_score': 6,
                'min_table_lines': 3,
                'header_freq': 3
            },
            ValidationLevel.LENIENT: {
                'page_weight': 0.7,
                'table_score': 7,
                'min_table_lines': 4,
                'header_freq': 2
            }
        }
        self.t: Dict = self.thresholds[level]

    def clean(self, text: str) -> str:
        if not text:
            return ""

        original_lines: int = text.count('\n') + 1

        text = self._remove_control_chars(text)
        text = self._convert_negatives(text)
        text = self._remove_pages(text)
        text = self._remove_headers_footers(text)
        text = self._remove_toc(text)
        text = self._process_tables(text)
        text = self._fix_line_breaks(text)
        text = self._normalize_whitespace(text)

        cleaned_lines: int = text.count('\n') + 1
        self.stats['removed_lines'] = original_lines - cleaned_lines

        return text

    def clean_with_validation(self, text: str) -> Tuple[str, Dict]:
        cleaned: str = self.clean(text)
        numbers: List[Dict] = self.extractor.extract_numbers(cleaned)
        numbers = self.deduplicator.deduplicate_numbers(numbers)
        report: Dict = self.validator.validate(text, cleaned, numbers)

        report['cleaner_stats'] = self.stats.copy()

        return cleaned, report

    def _remove_control_chars(self, text: str) -> str:
        return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)

    def _convert_negatives(self, text: str) -> str:
        lines: List[str] = text.split('\n')
        converted: List[str] = []

        for i, line in enumerate(lines):
            if self._is_financial_context(lines, i):
                def replace_neg(m: re.Match) -> str:
                    num: str = m.group(1).replace(',', '')
                    self.stats['converted_negatives'] += 1
                    return f"-{num}"

                line = re.sub(r'\(\s*([\d\.,]+)\s*\)', replace_neg, line)

            converted.append(line)

        return '\n'.join(converted)

    def _is_financial_context(self, lines: List[str], idx: int, window: int = 2) -> bool:
        start: int = max(0, idx - window)
        end: int = min(len(lines), idx + window + 1)

        for i in range(start, end):
            if self._is_financial_line(lines[i]):
                return True

        return False

    def _is_financial_line(self, line: str) -> bool:
        line_lower: str = line.lower()

        for kw, weight in PROTECTED_KEYWORDS.items():
            if kw in line_lower and weight >= 2:
                return True

        if re.search(r'rm[\d\.,]+|[\d\.,]+ (million|billion)|\d+%', line_lower):
            return True

        for header in COMPILED_FINANCIAL_HEADERS:
            if header.search(line):
                return True

        return False

    def _remove_pages(self, text: str) -> str:
        lines: List[str] = text.split('\n')
        cleaned: List[str] = []

        for i, line in enumerate(lines):
            keep: bool = True
            stripped: str = line.strip()

            if stripped and not self._is_financial_line(line):
                for pattern, weight in PAGE_PATTERNS:
                    if pattern.match(stripped) and weight >= self.t['page_weight']:
                        keep = False
                        self.stats['removed_pages'] += 1
                        break

            if keep:
                cleaned.append(line)

        return '\n'.join(cleaned)

    def _remove_headers_footers(self, text: str) -> str:
        lines: List[str] = text.split('\n')

        if len(lines) < 20:
            return text

        freq: Dict[str, int] = defaultdict(int)
        for line in lines[:10] + lines[-10:]:
            stripped: str = line.strip()
            if stripped and len(stripped) < 50 and not self._is_financial_line(line):
                freq[stripped] += 1

        common: Set[str] = {k for k, v in freq.items() if v >= self.t['header_freq']}

        cleaned: List[str] = []
        for line in lines:
            stripped = line.strip()

            if stripped in common and not self._is_financial_line(line):
                self.stats['removed_headers'] += 1
                continue

            match: bool = False
            for p in COMPILED_HEADER:
                if p.match(stripped) and not self._is_financial_line(line):
                    match = True
                    self.stats['removed_headers'] += 1
                    break

            if not match:
                cleaned.append(line)

        return '\n'.join(cleaned)

    def _remove_toc(self, text: str) -> str:
        patterns: List[Tuple[str, int]] = [
            (r'TABLE OF CONTENTS.*?(?=\n\d+\s+[A-Z])', re.DOTALL | re.IGNORECASE),
            (r'CONTENTS.*?(?=\n\d+\s+[A-Z])', re.DOTALL | re.IGNORECASE),
            (r'CONTENTS.*?(?=\n[A-Z][A-Z\s]+)', re.DOTALL | re.IGNORECASE),
            (r'目\s*录.*?(?=\n一、)', re.DOTALL)
        ]

        for pattern, flags in patterns:
            text = re.sub(pattern, '\n', text, flags=flags)

        return text

    def _score_table_line(self, line: str) -> int:
        stripped: str = line.strip()

        if not stripped or len(stripped) < 15:
            return 0

        score: int = 0

        parts: List[str] = re.split(r'\s{3,}', stripped)
        if len(parts) >= 3:
            score += 2
        if len(parts) >= 4:
            score += 1

        numbers: List[str] = re.findall(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b|\b\d+\.\d+\b', stripped)
        if len(numbers) >= 2:
            score += 2
            if len(numbers) >= 3:
                score += 1
            if all(len(n.replace(',', '')) >= 4 for n in numbers[:3]):
                score += 1

        if re.search(r'[A-Z]{4,}', stripped):
            score += 1

        if re.search(r'(RM|\$|£|€|%|million|billion|thousand)', stripped, re.IGNORECASE):
            score += 1

        if re.search(r'^\s*[A-Z][a-z]+\s+[A-Z][a-z]+', stripped):
            score -= 2

        if re.search(r'[.!?]$', stripped):
            score -= 3

        if re.search(r'page|see|refer|note|figure', stripped, re.IGNORECASE):
            score -= 2

        return max(0, score)

    def _is_table_line(self, line: str) -> bool:
        return self._score_table_line(line) >= self.t['table_score']

    def _find_table_regions(self, lines: List[str]) -> List[Tuple[int, int]]:
        regions: List[Tuple[int, int]] = []
        start: Optional[int] = None
        count: int = 0

        for i, line in enumerate(lines):
            if self._is_table_line(line):
                if start is None:
                    start = i
                count += 1
            else:
                if start is not None and count >= self.t['min_table_lines']:
                    regions.append((start, i))
                start = None
                count = 0

        if start is not None and count >= self.t['min_table_lines']:
            regions.append((start, len(lines)))

        return regions

    def _process_tables(self, text: str) -> str:
        lines: List[str] = text.split('\n')
        regions: List[Tuple[int, int]] = self._find_table_regions(lines)

        in_region: List[bool] = [False] * len(lines)
        for start, end in regions:
            for i in range(start, end):
                in_region[i] = True

        cleaned: List[str] = []
        i: int = 0

        while i < len(lines):
            if in_region[i]:
                table_lines: List[str] = []
                has_important: bool = False

                while i < len(lines) and in_region[i]:
                    line: str = lines[i]
                    table_lines.append(line)

                    if self._is_financial_line(line):
                        has_important = True

                    i += 1

                if has_important:
                    cleaned.extend(table_lines)
                    self.stats['protected_tables'] += len(table_lines)
            else:
                cleaned.append(lines[i])
                i += 1

        return '\n'.join(cleaned)

    def _fix_line_breaks(self, text: str) -> str:
        text = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)

        lines: List[str] = text.split('\n')
        fixed: List[str] = []
        i: int = 0

        while i < len(lines):
            curr: str = lines[i].rstrip()

            if (i < len(lines) - 1 and
                    curr and
                    curr[-1].isalpha() and
                    lines[i + 1].strip() and
                    lines[i + 1][0].islower()):

                fixed.append(curr + ' ' + lines[i + 1].lstrip())
                i += 2
            else:
                fixed.append(curr)
                i += 1

        return '\n'.join(fixed)

    def _normalize_whitespace(self, text: str) -> str:
        lines: List[str] = text.split('\n')
        normalized: List[str] = []

        for line in lines:
            if self._is_table_line(line):
                normalized.append(line.rstrip())
            else:
                line = re.sub(r'\s+', ' ', line).strip()
                if line:
                    normalized.append(line)

        result: List[str] = []
        empty_count: int = 0

        for line in normalized:
            if not line:
                empty_count += 1
                if empty_count <= 2:
                    result.append('')
            else:
                empty_count = 0
                result.append(line)

        return '\n'.join(result)

    def extract_sections(self, text: str) -> Dict[str, str]:
        sections: Dict[str, str] = {}
        lines: List[str] = text.split('\n')

        current: Optional[str] = None
        content: List[str] = []

        for line in lines:
            stripped: str = line.strip()
            is_section: bool = False

            for pattern in COMPILED_SECTION:
                if pattern.match(stripped):
                    if current and content:
                        sections[current] = '\n'.join(content).strip()

                    current = stripped
                    content = []
                    is_section = True
                    break

            if not is_section and current:
                content.append(line)

        if current and content:
            sections[current] = '\n'.join(content).strip()

        sections = self.deduplicator.merge_duplicate_sections(sections)

        return sections

    def get_stats(self) -> Dict:
        return self.stats.copy()


class AnnualReportChunker:
    def __init__(self, max_size: int = MAX_CONTENT_SIZE) -> None:
        self.max_size: int = max_size
        self.encoding: Optional[Any]
        self.has_tokenizer: bool
        self.encoding, self.has_tokenizer = init_tokenizer()
        self.stats: Dict[str, int] = {'total_chunks': 0, 'total_tokens': 0}
        self.deduplicator: DataDeduplicator = DataDeduplicator()

    def count_tokens(self, text: str) -> int:
        if self.has_tokenizer and self.encoding:
            return len(self.encoding.encode(text))

        return len(text) // 4

    def detect_sections(self, text: str) -> List[Tuple[int, int, str]]:
        lines: List[str] = text.split('\n')
        sections: List[Tuple[int, int, str]] = []

        current: Optional[str] = None
        start: int = 0

        for i, line in enumerate(lines):
            stripped: str = line.strip()

            for pattern in COMPILED_SECTION:
                if pattern.match(stripped):
                    if current:
                        sections.append((start, i, current))

                    current = stripped
                    start = i
                    break

        if current:
            sections.append((start, len(lines), current))

        return sections

    def chunk(self, text: str, meta: Dict = None) -> List[ChunkInfo]:
        meta = meta or {}

        sections: List[Tuple[int, int, str]] = self.detect_sections(text)

        if not sections:
            return self._chunk_by_tokens(text, meta)

        lines: List[str] = text.split('\n')
        chunks: List[ChunkInfo] = []

        for start, end, name in sections:
            section_lines: List[str] = lines[start:end]
            title: str = section_lines[0]
            content: str = '\n'.join(section_lines[1:])

            content_chunks: List[ChunkInfo] = self._chunk_by_tokens(content, meta)

            for i, chunk in enumerate(content_chunks):
                if i == 0:
                    full_text: str = title + '\n' + chunk.text
                    sections_list: List[str] = [name]
                else:
                    full_text = chunk.text
                    sections_list = []

                chunks.append(ChunkInfo(
                    id=len(chunks),
                    text=full_text,
                    tokens=self.count_tokens(full_text),
                    sections=sections_list,
                    meta=meta
                ))

        unique_texts: List[str] = self.deduplicator.deduplicate_texts([c.text for c in chunks])

        final_chunks: List[ChunkInfo] = []
        seen_texts: Set[str] = set()

        for chunk in chunks:
            if chunk.text in seen_texts:
                continue
            if chunk.text in unique_texts:
                final_chunks.append(chunk)
                seen_texts.add(chunk.text)

        for i, chunk in enumerate(final_chunks):
            chunk.id = i

        self.stats['total_chunks'] = len(final_chunks)
        self.stats['total_tokens'] = sum(c.tokens for c in final_chunks)

        return final_chunks

    def _chunk_by_tokens(self, text: str, meta: Dict) -> List[ChunkInfo]:
        lines: List[str] = text.split('\n')
        chunks: List[ChunkInfo] = []

        current_lines: List[str] = []
        current_tokens: int = 0

        for line in lines:
            tokens: int = self.count_tokens(line)

            if current_tokens + tokens > self.max_size and current_lines:
                chunk_text: str = '\n'.join(current_lines)
                chunks.append(ChunkInfo(
                    id=len(chunks),
                    text=chunk_text,
                    tokens=current_tokens,
                    meta=meta
                ))
                current_lines = [line]
                current_tokens = tokens
            else:
                current_lines.append(line)
                current_tokens += tokens

        if current_lines:
            chunk_text = '\n'.join(current_lines)
            chunks.append(ChunkInfo(
                id=len(chunks),
                text=chunk_text,
                tokens=current_tokens,
                meta=meta
            ))

        return chunks

    def chunk_file(self, path: str, out_dir: str = None, meta: Dict = None) -> List[ChunkInfo]:
        with read_file(path) as text:
            meta = meta or {}
            meta['source'] = Path(path).name

            chunks: List[ChunkInfo] = self.chunk(text, meta)

            if out_dir:
                Path(out_dir).mkdir(exist_ok=True)

                for chunk in chunks:
                    out_path: Path = Path(out_dir) / f"{Path(path).stem}_chunk_{chunk.id:03d}.txt"

                    with write_file(str(out_path)) as f:
                        f.write(chunk.text)

            return chunks


def clean_file(path: str, out_path: str = None, level: ValidationLevel = ValidationLevel.NORMAL) -> Tuple[str, Dict]:
    cleaner: AnnualReportCleaner = AnnualReportCleaner(level)

    with read_file(path) as text:
        cleaned, report = cleaner.clean_with_validation(text)

    if out_path is None:
        in_path = Path(path)
        out_path = str(in_path.parent / f"{in_path.stem}_cleaned{in_path.suffix}")

    with write_file(out_path) as f:
        f.write(cleaned)

    report_path: Path = Path(out_path).parent / f"{Path(out_path).stem}_report.json"
    with write_file(str(report_path)) as f:
        json.dump(report, f, indent=2, default=str)

    report_text: str = cleaner.validator.generate_report_text(report)
    txt_report_path: Path = Path(out_path).parent / f"{Path(out_path).stem}_report.txt"
    with write_file(str(txt_report_path)) as f:
        f.write(report_text)

    logger.info(f"Cleaned: {path} -> {out_path}")
    logger.info(f"Reduction: {report['stats']['reduction_pct']}%")

    if report['errors']:
        logger.warning(f"Errors: {len(report['errors'])}")
    if report['warnings']:
        logger.warning(f"Warnings: {len(report['warnings'])}")

    return cleaned, report


def batch_clean(in_dir: str, out_dir: str = 'cleaned', pattern: str = "*.txt",
                level: ValidationLevel = ValidationLevel.NORMAL) -> pd.DataFrame:
    Path(out_dir).mkdir(exist_ok=True)

    in_path = Path(in_dir)
    files: List[Path] = list(in_path.glob(pattern))

    logger.info(f"Found {len(files)} files")

    results: List[Dict] = []
    cleaner: AnnualReportCleaner = AnnualReportCleaner(level)

    for i, file_path in enumerate(files, 1):
        logger.info(f"[{i}/{len(files)}] Processing: {file_path.name}")

        try:
            with read_file(str(file_path)) as text:
                cleaned, report = cleaner.clean_with_validation(text)

            out_file: Path = Path(out_dir) / f"cleaned_{file_path.name}"
            with write_file(str(out_file)) as f:
                f.write(cleaned)

            report_dir: Path = Path(out_dir) / "reports"
            report_dir.mkdir(exist_ok=True)

            report_path: Path = report_dir / f"{file_path.stem}_report.json"
            with write_file(str(report_path)) as f:
                json.dump(report, f, indent=2, default=str)

            sections: Dict[str, str] = cleaner.extract_sections(cleaned)
            if sections:
                sec_dir: Path = Path(out_dir) / "sections"
                sec_dir.mkdir(exist_ok=True)

                for name, sec_text in sections.items():
                    safe_name: str = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')
                    sec_path: Path = sec_dir / f"{file_path.stem}_{safe_name}.txt"

                    with write_file(str(sec_path)) as f:
                        f.write(sec_text)

            results.append({
                'file': file_path.name,
                'original_chars': report['stats']['original_chars'],
                'cleaned_chars': report['stats']['cleaned_chars'],
                'reduction_pct': report['stats']['reduction_pct'],
                'original_lines': report['stats']['original_lines'],
                'cleaned_lines': report['stats']['cleaned_lines'],
                'warnings': len(report.get('warnings', [])),
                'errors': len(report.get('errors', [])),
                'numbers': report.get('numbers', {}).get('total', 0),
                'negatives': report.get('numbers', {}).get('negatives', 0),
                'duplicates_removed': report.get('duplicates', {}).get('removed', 0)
            })

            logger.info(f"  OK - Reduction: {report['stats']['reduction_pct']}%")

        except Exception as e:
            logger.error(f"  Failed: {e}")
            results.append({
                'file': file_path.name,
                'error': str(e)
            })

    df: pd.DataFrame = pd.DataFrame(results)
    df.to_csv(Path(out_dir) / "batch_clean_report.csv", index=False, encoding='utf-8-sig')

    return df


def batch_chunk(cleaned_dir: str = 'cleaned', chunks_dir: str = 'chunks',
                pattern: str = "cleaned_*.txt") -> pd.DataFrame:
    Path(chunks_dir).mkdir(exist_ok=True)

    cleaned_path = Path(cleaned_dir)
    files: List[Path] = list(cleaned_path.glob(pattern))

    if not files:
        logger.warning(f"No files matching {pattern} in {cleaned_dir}")
        return pd.DataFrame()

    logger.info(f"Found {len(files)} files")

    chunker: AnnualReportChunker = AnnualReportChunker()
    all_chunks: List[Dict] = []

    for i, file_path in enumerate(files, 1):
        logger.info(f"[{i}/{len(files)}] Chunking: {file_path.name}")

        try:
            meta: Dict = {'source': file_path.name}

            name_parts: List[str] = file_path.stem.replace('cleaned_', '').split('_')

            if len(name_parts) >= 2:
                code_match: Optional[re.Match] = re.search(r'([A-Z]+)', name_parts[0])
                if code_match:
                    meta['code'] = code_match.group(1)

                year_match: Optional[re.Match] = re.search(r'(20\d{2}|\d{2})', file_path.name)
                if year_match:
                    year_str: str = year_match.group(1)
                    if len(year_str) == 2:
                        year: int = 2000 + int(year_str) if int(year_str) <= 30 else 1900 + int(year_str)
                        meta['year'] = year
                    else:
                        meta['year'] = int(year_str)

            chunks: List[ChunkInfo] = chunker.chunk_file(str(file_path), chunks_dir, meta)

            for chunk in chunks:
                all_chunks.append({
                    'id': chunk.id,
                    'source': file_path.name,
                    'tokens': chunk.tokens,
                    'sections': '|'.join(chunk.sections),
                    'code': meta.get('code', ''),
                    'year': meta.get('year', ''),
                    'file_path': str(Path(chunks_dir) / f"{file_path.stem}_chunk_{chunk.id:03d}.txt")
                })

            logger.info(f"  OK - {len(chunks)} chunks, {chunker.stats['total_tokens']:,} tokens")

        except Exception as e:
            logger.error(f"  Failed: {e}")

    df: pd.DataFrame = pd.DataFrame(all_chunks)

    if not df.empty:
        df.to_csv(Path(chunks_dir) / "chunks_summary.csv", index=False, encoding='utf-8-sig')
        logger.info(f"Total chunks: {len(df)}, Total tokens: {df['tokens'].sum():,}")

    return df


def extract_financial_data(cleaned_dir: str = 'cleaned', output_dir: str = 'financial_data',
                           pattern: str = "cleaned_*.txt") -> pd.DataFrame:
    Path(output_dir).mkdir(exist_ok=True)

    cleaned_path = Path(cleaned_dir)
    files: List[Path] = list(cleaned_path.glob(pattern))

    if not files:
        logger.warning(f"No files matching {pattern} in {cleaned_dir}")
        return pd.DataFrame()

    logger.info(f"Found {len(files)} files")

    extractor: FinancialDataExtractor = FinancialDataExtractor()
    deduplicator: DataDeduplicator = DataDeduplicator()

    all_numbers: List[Dict] = []

    for file_path in files:
        logger.info(f"Extracting from: {file_path.name}")

        with read_file(str(file_path)) as text:
            numbers: List[Dict] = extractor.extract_numbers(text)
            numbers = deduplicator.deduplicate_numbers(numbers)

            for n in numbers:
                n['source'] = file_path.name
                all_numbers.append(n)

            df_file: pd.DataFrame = pd.DataFrame(numbers)
            df_file.to_csv(Path(output_dir) / f"{file_path.stem}_numbers.csv", index=False, encoding='utf-8-sig')

            logger.info(f"  Extracted {len(numbers)} numbers")

    if all_numbers:
        df_all: pd.DataFrame = pd.DataFrame(all_numbers)
        df_all.to_csv(Path(output_dir) / "all_numbers.csv", index=False, encoding='utf-8-sig')

        outlier_detector: OutlierDetector = OutlierDetector()
        outlier_report: Dict = outlier_detector.get_outlier_report(all_numbers)

        with write_file(str(Path(output_dir) / "outliers.json")) as f:
            json.dump(outlier_report, f, indent=2, default=str)

        logger.info(f"Total numbers: {len(all_numbers)}")
        logger.info(f"Outliers detected: {outlier_report.get('total', 0)}")

        return df_all

    return pd.DataFrame()


def pipeline(in_dir: str, cleaned_dir: str = 'cleaned', chunks_dir: str = 'chunks',
             financial_dir: str = 'financial_data', pattern: str = "*.txt",
             level: ValidationLevel = ValidationLevel.NORMAL) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("=" * 60)
    logger.info("STARTING PIPELINE")
    logger.info(f"Level: {level.value}")
    logger.info("=" * 60)

    logger.info("Step 1: Cleaning")
    clean_df: pd.DataFrame = batch_clean(in_dir, cleaned_dir, pattern, level)

    logger.info("\nStep 2: Chunking")
    chunks_df: pd.DataFrame = batch_chunk(cleaned_dir, chunks_dir)

    logger.info("\nStep 3: Financial Data Extraction")
    financial_df: pd.DataFrame = extract_financial_data(cleaned_dir, financial_dir)

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)

    return clean_df, chunks_df, financial_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Annual Report Cleaning Pipeline')

    parser.add_argument('--input', '-i', required=True, help='Input file or directory')
    parser.add_argument('--output', '-o', default='cleaned', help='Output directory')
    parser.add_argument('--chunks', '-c', default='chunks', help='Chunks output directory')
    parser.add_argument('--financial', '-f', default='financial_data', help='Financial data output directory')
    parser.add_argument('--pattern', '-p', default='*.txt', help='File pattern for batch processing')
    parser.add_argument('--level', '-l', choices=['strict', 'normal', 'lenient'],
                        default='normal', help='Validation level')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--batch', '-b', action='store_true', help='Batch process directory')
    group.add_argument('--chunk-only', action='store_true', help='Only chunk cleaned files')
    group.add_argument('--extract-only', action='store_true', help='Only extract financial data')
    group.add_argument('--pipeline', action='store_true', help='Run full pipeline')

    args = parser.parse_args()

    level_map: Dict[str, ValidationLevel] = {
        'strict': ValidationLevel.STRICT,
        'normal': ValidationLevel.NORMAL,
        'lenient': ValidationLevel.LENIENT
    }
    level: ValidationLevel = level_map[args.level]

    if args.chunk_only:
        batch_chunk(args.input, args.output)

    elif args.extract_only:
        extract_financial_data(args.input, args.output)

    elif args.pipeline or args.batch:
        if os.path.isdir(args.input):
            pipeline(args.input, args.output, args.chunks, args.financial, args.pattern, level)
        else:
            logger.error(f"Input must be a directory for pipeline/batch mode: {args.input}")

    else:
        if os.path.isfile(args.input):
            clean_file(args.input, level=level)
        else:
            logger.error(f"Input file not found: {args.input}")