from typing import List, Dict
import pandas as pd
import dbf
import re
import datetime

# -----------------------------------------------------------
# Настройки файлов и кодировок
IN_DB = 'NKVD01.DBF'
NKVD03_DB = 'NKVD03.DBF'
NKVD04_DB = 'NKVD04.DBF'
NKVD05_DB = 'NKVD05.DBF'
NKVD06_DB = 'NKVD06.DBF'
OUT_DB = 'NKVD01_new.DBF'
ENCODING_IN = 'cp866'
ENCODING_OUT = 'cp1251'

# Даты (day, month, year)
DATE_GROUPS: Dict[str, List[str]] = {
    'DB': ['DB1', 'DB2', 'DB3'],
    'DA': ['DA1', 'DA2', 'DA3'],
    'DI': ['DI1', 'DI2', 'DI3'],
    'DC': ['DC1', 'DC2', 'DC3'],
}

ugd_merge = ['VD1', 'GOD', 'KOD', 'UGD', 'TER']

# ST*/PUNKT
F1 = "ST1_ZN_CH"
F2 = "P1_PUNKT"
F3 = "ST2_ZN_CH"
F4 = "P2_PUNKT"
F5 = "ST3_ZN_CH"
F6 = "P3_PUNKT"

# Разделители
SFE_SEPARATOR = ";"
LIN_SEPARATOR = ";"
LI2_SEPARATOR = ";"

# -----------------------------------------------------------
def combine_date_parts(fields, row):
    """
    Нормализация даты: если все три поля пусты -> возвращаем пустую строку ''.
    Иначе пытаемся собрать dd.mm.yyyy, при ошибках возвращаем "01.01.1900".
    """
    raw = []
    for f in fields:
        v = row.get(f, '')
        if pd.isna(v):
            v = ''
        raw.append(str(v).strip())

    day, month, year = raw

    # если все пустые — оставляем пустое поле
    if (not day) and (not month) and (not year):
        return ''

    # если год указан, он должен быть числом длиной 2 или 4
    if year and (not year.isdigit() or len(year) not in (2, 4)):
        return "01.01.1900"

    if day and not day.isdigit():
        return "01.01.1900"
    if month and not month.isdigit():
        return "01.01.1900"

    # подстановка недостающих частей, если указан год
    if year and not day and not month:
        day = '01'
        month = '01'
    if year and month and not day:
        day = '01'
    if year and day and not month:
        month = '01'

    # если год не указан, но день/месяц указаны — считем это ошибкой (возвращаем 01.01.1900)
    if not year:
        return "01.01.1900"

    day = day.zfill(2) if day else '01'
    month = month.zfill(2) if month else '01'
    year = year.zfill(4)

    try:
        d = int(day); m = int(month)
        if not (1 <= d <= 31 and 1 <= m <= 12):
            return "01.01.1900"
    except:
        return "01.01.1900"

    return f"{day}.{month}.{year}"

# -----------------------------------------------------------
def read_dbf_with_all_records(path: str, encoding: str = ENCODING_IN) -> pd.DataFrame:
    table = dbf.Table(path, codepage=encoding)
    table.open()
    records = []
    for rec in table:
        row = {}
        for field in table.field_names:
            row[field] = rec[field]
        records.append(row)
    df = pd.DataFrame(records)
    total_records = len(table)
    if len(df) < total_records:
        empty_df = pd.DataFrame([{c: None for c in df.columns}] * (total_records - len(df)))
        df = pd.concat([df, empty_df], ignore_index=True)
    table.close()
    return df

# -----------------------------------------------------------
def normalize_digits(s) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ''
    s = str(s)
    return ''.join(re.findall(r'\d', s))

def build_st_zn_ch(sta, zna, cha) -> str:
    sta_d = normalize_digits(sta)
    zna_d = normalize_digits(zna)
    zna_nozeros = zna_d.replace('0', '') or '0'
    cha_d = normalize_digits(cha)
    if not (sta_d or zna_nozeros or cha_d):
        return ''
    return f"{sta_d}0{zna_nozeros}{cha_d}"

# -----------------------------------------------------------
def build_nkvd03_map(nkvd03_df: pd.DataFrame) -> Dict[str, List[Dict[str, str]]]:
    mapping = {}
    for _, row in nkvd03_df.iterrows():
        key = str(row.get('P99999', '')).strip()
        entry = {
            'STA': '' if pd.isna(row.get('STA')) else row.get('STA'),
            'ZNA': '' if pd.isna(row.get('ZNA')) else row.get('ZNA'),
            'CHA': '' if pd.isna(row.get('CHA')) else row.get('CHA'),
            'PUN': '' if pd.isna(row.get('PUN')) else row.get('PUN'),
        }
        mapping.setdefault(key, []).append(entry)
    return mapping

def build_nkvd04_multi(nkvd04_df: pd.DataFrame) -> Dict[str, List[str]]:
    multi_map: Dict[str, List[str]] = {}
    for _, row in nkvd04_df.iterrows():
        key = str(row.get('P99999', '')).strip()
        sfe_val = row.get('SFE', '')
        if pd.isna(sfe_val):
            continue
        sfe_s = str(sfe_val).strip()
        if sfe_s == '':
            continue
        multi_map.setdefault(key, []).append(sfe_s)
    return multi_map

def build_nkvd05_multi(nkvd05_df: pd.DataFrame) -> Dict[str, List[str]]:
    multi_map: Dict[str, List[str]] = {}
    for _, row in nkvd05_df.iterrows():
        key = str(row.get('P99999', '')).strip()
        lin_val = row.get('LIN', '')
        if pd.isna(lin_val):
            continue
        lin_s = str(lin_val).strip()
        if lin_s == '':
            continue
        multi_map.setdefault(key, []).append(lin_s)
    return multi_map

def build_nkvd06_multi(nkvd06_df: pd.DataFrame) -> Dict[str, List[str]]:
    multi_map: Dict[str, List[str]] = {}
    for _, row in nkvd06_df.iterrows():
        key = str(row.get('P99999', '')).strip()
        li2_val = None
        for candidate in ('LI2', 'LI', 'L2', 'VAL', 'VALUE'):
            try:
                li2_val = row.get(candidate)
            except Exception:
                li2_val = None
            if li2_val is not None:
                break
        if li2_val is None:
            li2_val = row.get('LI2', '')
        if pd.isna(li2_val):
            continue
        li2_s = str(li2_val).strip()
        if li2_s == '':
            continue
        multi_map.setdefault(key, []).append(li2_s)
    return multi_map

# -----------------------------------------------------------
def map_zav_primary(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        digits = '01'
    else:
        digits = ''.join(re.findall(r'\d', str(v))).strip()
        if not digits:
            digits = '01'
        digits = digits.zfill(2)[-2:]

    if digits == '03':
        return '300'
    if digits == '04':
        return '400'
    if digits == '05':
        return '500'
    return '100'

def map_poluch_iz(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        digits = ''
    else:
        digits = ''.join(re.findall(r'\d', str(v))).strip()
        if not digits:
            digits = ''
    if not digits:
        return ''
    digits = digits.zfill(2)[-2:]
    if digits == '06':
        return '012'
    if digits == '07':
        return '013'
    if digits == '17':
        return '017'
    return ''

# -----------------------------------------------------------
# LIN substitution sets (as requested)
LIN_TO_84 = {33,34,35,37,52}
LIN_TO_16 = {
    45,38,30,40,73,31,42,32,36,43,39,46,48,49,50,71,
    53,54,55,51,58,62,59,63,65,64,66,67,70,56,77,47,
    88,89,90,75,76,91,92,93,94,95,96,97,100,101,103,61,60
}
LIN_TO_88 = {44}
LIN_TO_12 = {57}

def normalize_lin_value(lin_raw: str) -> str:
    """
    Новая логика нормализации LIN:
     - извлекаем цифры
     - если попадает в замены — возвращаем замену (строка)
     - иначе удаляем только первый ведущий 0 (если есть) и возвращаем
    """
    if lin_raw is None:
        return ''
    s = str(lin_raw).strip()
    if s == '':
        return ''
    digits = ''.join(re.findall(r'\d', s))
    if digits == '':
        return ''
    try:
        n = int(digits)
    except:
        # fallback: удалить первый ноль если есть
        return (digits[1:] if digits and digits[0] == '0' else digits)
    if n in LIN_TO_84:
        return '84'
    if n in LIN_TO_16:
        return '16'
    if n in LIN_TO_88:
        return '88'
    if n in LIN_TO_12:
        return '12'
    # remove only first leading zero
    if digits and digits[0] == '0':
        return digits[1:]
    return digits

# -----------------------------------------------------------
def join_unique_preserve_order(items: List[str], sep: str) -> str:
    seen = set()
    out = []
    for it in items:
        if it is None:
            continue
        s = str(it).strip()
        if s == '':
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return sep.join(out)

# -----------------------------------------------------------
def should_include_ter_by_dc_date(dc_date: datetime.date) -> bool:
    """
    Возвращает True если TER следует добавлять в OLD-формате.
    Правило: если dc_date отсутствует -> True (поведение старое).
            если dc_date < 2017-02-01 -> True (старое — добавляем TER).
            если dc_date >= 2017-02-01 -> False (новый формат — не добавляем TER).
    """
    if dc_date is None:
        return True
    cutoff = datetime.date(2017, 2, 1)
    return dc_date < cutoff

def parse_date_from_dc_string(dc_str: str) -> datetime.date:
    """
    dc_str ожидается в формате dd.mm.yyyy или ''. Возвращает datetime.date или None.
    """
    if dc_str is None:
        return None
    s = str(dc_str).strip()
    if s == '':
        return None
    m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})$', s)
    if not m:
        return None
    try:
        day = int(m.group(1)); month = int(m.group(2)); year = int(m.group(3))
        return datetime.date(year, month, day)
    except:
        return None

def build_ugd_merge_for_row_using_dc(row) -> str:
    """
    Новая логика UGD_MERGE:
      - если DC >= 01.02.2017 => VD1 + GOD + KOD + UGD(6 digits zfilled)
      - иначе (DC < 01.02.2017 или DC пустое) => UGD(5 digits zfilled) + TER (если TER есть; перед добавлением у TER удаляем первый '0' если есть)
    UGD берётся из поля 'UGD' (строка), обнуляется слева до 5 или 6 цифр в зависимости от ветки.
    Остальные компоненты (VD1,GOD,KOD) при новой ветке конкатенируются как строки без пробелов.
    """
    # старая UGD_MERGE parts for compatibility if needed
    vd1 = row.get('VD1', '') or ''
    god = row.get('GOD', '') or ''
    kod = row.get('KOD', '') or ''
    ugd_raw = row.get('UGD', '') or ''
    ter_raw = row.get('TER', '') or ''

    # DC уже сформирован как строка dd.mm.yyyy или '' — парсим
    dc_str = row.get('DC', '')
    dc_date = parse_date_from_dc_string(dc_str)

    cutoff = datetime.date(2017, 2, 1)
    # если dc_date >= cutoff => новый формат (VD1+GOD+KOD+UGD(6))
    if dc_date is not None and dc_date >= cutoff:
        # format UGD to 6 digits (only digits kept)
        ugd_digits = ''.join(re.findall(r'\d', str(ugd_raw)))
        ugd6 = ugd_digits.zfill(6) if ugd_digits != '' else ''
        parts = [vd1, god, kod, ugd6]
        merged = ''.join(parts).replace(' ', '')
        return merged
    else:
        # старый формат: UGD 5 digits + TER_if_allowed
        ugd_digits = ''.join(re.findall(r'\d', str(ugd_raw)))
        ugd5 = ugd_digits.zfill(5) if ugd_digits != '' else ''
        merged = ugd5
        # добавляем TER если allowed (if dc_date is None or dc_date < cutoff)
        include_ter = should_include_ter_by_dc_date(dc_date)
        if include_ter and ter_raw is not None and str(ter_raw).strip() != '':
            ter_s = str(ter_raw)
            # удалить только первый символ, если он '0'
            if len(ter_s) > 0 and ter_s[0] == '0':
                ter_s = ter_s[1:]
            merged = (merged + ter_s)
        merged = merged.replace(' ', '')
        return merged

# -----------------------------------------------------------
def map_oss_field(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ''
    s = str(v).strip()
    if s == '':
        return ''
    digits = ''.join(re.findall(r'\d', s))
    if digits in ('08', '9', '09', '53', '56', '57', '58'):
        return '15'
    return s

def transform_kud(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ''
    s = str(v).strip()
    if s == '':
        return ''
    digits = ''.join(re.findall(r'\d', s))
    if digits == '':
        return ''
    digits_padded = digits.zfill(3)
    return '11150' + digits_padded

# -----------------------------------------------------------
def process_dataframe(df: pd.DataFrame,
                      nkvd03_map: Dict[str, List[Dict[str, str]]],
                      nkvd04_multi: Dict[str, List[str]],
                      nkvd05_multi: Dict[str, List[str]],
                      nkvd06_multi: Dict[str, List[str]]) -> pd.DataFrame:
    # ROW_NUM
    if 'ROW_NUM' not in df.columns:
        df['ROW_NUM'] = range(1, len(df)+1)

    # copy SNY
    df['SNY'] = df.get('SNY', '').fillna('').astype(str)

    # OVD
    if 'OVD' in df.columns:
        df['OVD'] = df['OVD'].fillna('').astype(str)
        df['OVD'] = df['OVD'].apply(lambda v: '11150' + (v if v.strip() != '' else '000'))

    # LI0 transform: keep previous rules, but map '04' -> '0010'
    if 'LI0' in df.columns:
        def li0_transform(v):
            s = ('' if pd.isna(v) else str(v).strip()) or '13'
            s = s.zfill(2)
            if s == '03':
                # previous special case: 03 -> 25
                return '0025'
            if s == '04':
                # requested mapping: 04 -> 0010
                return '0010'
            return '00' + s
        df['LI0'] = df['LI0'].apply(li0_transform)
    else:
        df['LI0'] = ''

    # VID_ED
    df['VID_ED'] = df['LI0'].apply(lambda v: '00001' if str(v) == '0010' else '')

    # Dates: DB, DA, DI, DC
    for new_field, parts in DATE_GROUPS.items():
        df[new_field] = df.apply(lambda row, p=parts: combine_date_parts(p, row), axis=1)

    # ensure ugd_merge cols exist
    for col in ugd_merge:
        if col not in df.columns:
            df[col] = ''

    # build UGD_MERGE using new logic (depends on DC string)
    df['UGD_MERGE'] = df.apply(build_ugd_merge_for_row_using_dc, axis=1)

    # ST*/PUNKT new fields
    df[F1] = ''
    df[F2] = ''
    df[F3] = ''
    df[F4] = ''
    df[F5] = ''
    df[F6] = ''

    # ZAV and POLUCH_IZ (restored)
    src_zav_col = 'ZAV' if 'ZAV' in df.columns else None
    if src_zav_col:
        orig_zav = df[src_zav_col].copy()
    else:
        orig_zav = pd.Series([None] * len(df), index=df.index)
    df['ZAV'] = orig_zav.apply(map_zav_primary)
    df['POLUCH_IZ'] = orig_zav.apply(map_poluch_iz)

    # fill ST*/PUNKT from nkvd03_map
    for idx, row in df.iterrows():
        key = str(row['ROW_NUM'])
        entries = nkvd03_map.get(key, [])
        for i in range(3):
            if i < len(entries):
                ent = entries[i]
                st_zn_ch = build_st_zn_ch(ent['STA'], ent['ZNA'], ent['CHA'])
                punkt = ent['PUN'] if ent['PUN'] else ''
                if i == 0:
                    df.at[idx, F1] = st_zn_ch
                    df.at[idx, F2] = punkt
                elif i == 1:
                    df.at[idx, F3] = st_zn_ch
                    df.at[idx, F4] = punkt
                elif i == 2:
                    df.at[idx, F5] = st_zn_ch
                    df.at[idx, F6] = punkt

    # SFE from nkvd04_multi
    def get_sfe_for_row(row):
        key = str(row.get('ROW_NUM', '')).strip()
        sfe_list = nkvd04_multi.get(key, [])
        if not sfe_list:
            return ''
        return join_unique_preserve_order(sfe_list, SFE_SEPARATOR)
    df['SFE'] = df.apply(get_sfe_for_row, axis=1)

    # LIN from nkvd05_multi with new normalization
    def get_lin_for_row(row):
        key = str(row.get('ROW_NUM', '')).strip()
        lin_list = nkvd05_multi.get(key, [])
        if not lin_list:
            return ''
        normalized = [normalize_lin_value(x) for x in lin_list if str(x).strip() != '']
        normalized = [x for x in normalized if x != '']
        if not normalized:
            return ''
        return join_unique_preserve_order(normalized, LIN_SEPARATOR)
    df['LIN'] = df.apply(get_lin_for_row, axis=1)

    # LI2 from nkvd06_multi
    def get_li2_for_row(row):
        key = str(row.get('ROW_NUM', '')).strip()
        li2_list = nkvd06_multi.get(key, [])
        if not li2_list:
            return ''
        return join_unique_preserve_order(li2_list, LI2_SEPARATOR)
    df['LI2'] = df.apply(get_li2_for_row, axis=1)

    # DD and SN from respective triples
    df['DD'] = df.apply(lambda row: combine_date_parts(['DD1','DD2','DD3'], row), axis=1)
    df['SN'] = df.apply(lambda row: combine_date_parts(['SN1','SN2','SN3'], row), axis=1)

    # OSS, KUD, ARX, DR, FAI, DOP, RE, RE2 (unchanged behavior)
    df['OSS'] = df.get('OSS', '').apply(lambda v: map_oss_field(v) if v is not None else '')
    df['KUD'] = df.get('KUD', '').apply(lambda v: transform_kud(v) if v is not None else '')
    df['ARX'] = df.get('ARX', '').fillna('').astype(str)
    df['DR'] = df.apply(lambda row: combine_date_parts(['DR1','DR2','DR3'], row), axis=1)
    df['FAI'] = df.get('FAI', '').fillna('').astype(str)
    df['DOP'] = df.get('DOP', '').fillna('').astype(str)
    df['RE'] = df.apply(lambda row: combine_date_parts(['RE1','RE2','RE3'], row), axis=1)
    df['RE2'] = df.apply(lambda row: combine_date_parts(['RE1','RE2','RE3'], row), axis=1)

    return df

# -----------------------------------------------------------
def write_dbf(df: pd.DataFrame, path: str, encoding: str = ENCODING_OUT):
    field_specs = []
    for col in df.columns:
        max_len = df[col].astype(str).map(len).max()
        try:
            max_len = int(max_len)
        except:
            max_len = 10
        max_len = max(max_len, 10)
        spec = f"{col} C({max_len})"
        field_specs.append(spec)
    spec = ';'.join(field_specs)

    table = dbf.Table(path, spec, codepage=encoding)
    table.open(dbf.READ_WRITE)

    for _, row in df.iterrows():
        rec = {c: ('' if pd.isna(row[c]) else str(row[c])) for c in df.columns}
        table.append(rec)

    table.close()

# -----------------------------------------------------------
def main():
    print(f"[read] Чтение {IN_DB} ...")
    df = read_dbf_with_all_records(IN_DB, encoding=ENCODING_IN)
    print(f"[info] Прочитано {len(df)} записей, столбцы: {list(df.columns)}")

    print(f"[read] Чтение {NKVD03_DB} ...")
    nkvd03_df = read_dbf_with_all_records(NKVD03_DB, encoding=ENCODING_IN)
    print(f"[info] NKVD03: прочитано {len(nkvd03_df)} записей, столбцы: {list(nkvd03_df.columns)}")

    print(f"[read] Чтение {NKVD04_DB} ...")
    nkvd04_df = read_dbf_with_all_records(NKVD04_DB, encoding=ENCODING_IN)
    print(f"[info] NKVD04: прочитано {len(nkvd04_df)} записей, столбцы: {list(nkvd04_df.columns)}")

    print(f"[read] Чтение {NKVD05_DB} ...")
    nkvd05_df = read_dbf_with_all_records(NKVD05_DB, encoding=ENCODING_IN)
    print(f"[info] NKVD05: прочитано {len(nkvd05_df)} записей, столбцы: {list(nkvd05_df.columns)}")

    print(f"[read] Чтение {NKVD06_DB} ...")
    nkvd06_df = read_dbf_with_all_records(NKVD06_DB, encoding=ENCODING_IN)
    print(f"[info] NKVD06: прочитано {len(nkvd06_df)} записей, столбцы: {list(nkvd06_df.columns)}")

    nkvd03_map = build_nkvd03_map(nkvd03_df)
    nkvd04_multi = build_nkvd04_multi(nkvd04_df)
    nkvd05_multi = build_nkvd05_multi(nkvd05_df)
    nkvd06_multi = build_nkvd06_multi(nkvd06_df)

    df = process_dataframe(df, nkvd03_map, nkvd04_multi, nkvd05_multi, nkvd06_multi)

    # Итоговый порядок полей (включил ZAV и POLUCH_IZ, остальные поля не тронуты)
    columns_to_keep = [
        'ROW_NUM', 'OVD', 'LI0', 'VID', 'NOM', 'DB', 'DA', 'DI',
        'FAB', 'ZAV', 'POLUCH_IZ',
        F1, F2, F3, F4, F5, F6,
        'SFE', 'LIN', 'LI2', 'SNY', 'DD', 'SN', 'VID_ED',
        'UGD_MERGE', 'DC',
        'OSS', 'KUD', 'ARX', 'DR', 'FAI', 'DOP', 'RE', 'RE2'
    ]

    df_to_write = df[[c for c in columns_to_keep if c in df.columns]].copy()

    print(f"[write] Создаём файл: {OUT_DB}")
    write_dbf(df_to_write, OUT_DB, encoding=ENCODING_OUT)
    print(f"[ok] Pipeline завершён успешно. Записано полей: {columns_to_keep}")

# -----------------------------------------------------------
if __name__ == "__main__":
    main()
