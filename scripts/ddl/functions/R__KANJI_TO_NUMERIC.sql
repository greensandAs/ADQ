USE DATABASE {{ snowflake_database }};

USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE FUNCTION "KANJI_TO_NUMERIC"("KJ_STR" VARCHAR)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'run'
AS '
def run(kj_str):
    if kj_str is None:
        return None

    s = kj_str.strip()
    if not s:
        return None

    try:
        return float(s)
    except ValueError:
        pass

    # MODIFIED: Added alternative zero ''〇'' to the dictionary.
    nums = {''零'':0, ''〇'':0, ''一'':1, ''二'':2, ''三'':3, ''四'':4, ''五'':5, ''六'':6, ''七'':7, ''八'':8, ''九'':9}
    units = {''十'':10, ''百'':100, ''千'':1000}
    big_units = {''万'':10000, ''億'':100000000, ''兆'':1000000000000}

    negative = False
    if s.startswith(''−'') or s.startswith(''-''):
        negative = True
        s = s[1:]

    # MODIFIED: Replace alternative decimal separator ''・'' with the standard ''点''.
    s = s.replace(''・'', ''点'')

    if ''点'' in s:
        integer_part, decimal_part = s.split(''点'', 1)
    else:
        integer_part, decimal_part = s, ''''

    def parse_integer_part(p):
        total = 0
        section = 0
        number = 0
        for c in p:
            if c in nums:
                number = nums[c]
            elif c in units:
                unit_val = units[c]
                if number == 0:
                    number = 1
                section += number * unit_val
                number = 0
            elif c in big_units:
                unit_val = big_units[c]
                if number != 0 or section == 0:
                    section += number
                total += section * unit_val
                section = 0
                number = 0
            else:
                # unsupported char
                return None
        total += section + number
        return total

    int_val = parse_integer_part(integer_part)
    if int_val is None:
        return None

    decimal_val = 0.0
    if decimal_part:
        dec_str = ''''
        for c in decimal_part:
            if c in nums:
                dec_str += str(nums[c])
            else:
                return None
        if dec_str:
            decimal_val = float(''0.'' + dec_str)

    result = int_val + decimal_val
    if negative:
        result = -result

    return float(result)
';
