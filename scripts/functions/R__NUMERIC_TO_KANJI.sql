USE DATABASE {{ snowflake_database }};
USE SCHEMA {{ snowflake_schema }};

CREATE OR REPLACE FUNCTION "NUMERIC_TO_KANJI"("NUM" FLOAT)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'run'
AS '
def run(num):
    if num is None:
        return None

    digits = [''零'', ''一'', ''二'', ''三'', ''四'', ''五'', ''六'', ''七'', ''八'', ''九'']
    units = ['''', ''十'', ''百'', ''千'']
    big_units = ['''', ''万'', ''億'', ''兆'']

    if num == 0:
        return digits[0]

    negative = False
    if num < 0:
        negative = True
        num = -num

    integer_part = int(num)
    decimal_part = num - integer_part

    def four_digit_to_kanji(num):
        result = ''''
        str_num = str(int(num)).zfill(4)
        for i, ch in enumerate(str_num):
            d = int(ch)
            if d != 0:
                # omit ''一'' before unit except for last digit
                if not (d == 1 and i != 3):
                    result += digits[d]
                result += units[3 - i]
        return result

    str_num = str(integer_part)[::-1]
    groups = [str_num[i:i+4][::-1] for i in range(0, len(str_num), 4)]

    result = ''''
    for i in range(len(groups)):
        part = int(groups[i])
        if part == 0:
            continue
        part_kanji = four_digit_to_kanji(part)
        result = part_kanji + big_units[i] + result

    # Process decimal part
    if decimal_part > 0:
        dec_str = ''点''
        decimal_str = str(decimal_part)[2:12]  # up to 10 decimal digits
        for ch in decimal_str:
            dec_str += digits[int(ch)]
        result += dec_str

    if negative:
        result = ''−'' + result

    return result
';;
