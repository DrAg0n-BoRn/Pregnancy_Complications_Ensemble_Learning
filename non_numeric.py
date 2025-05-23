## Transform to numeric values
## Missing values filled using MICE later on

import polars as pl
import os

non_numeric_data_path = os.path.join(os.getcwd(), "outputs", "raw_non_numeric_data.csv")
non_numeric_data_v2_path = os.path.join(os.getcwd(), "outputs", "processed_non_numeric_data.csv")
ID_COL = '匹配ID_日期'

#Define rules
def 主要治疗结果(values: pl.Series):
    '''
    治愈=3, 好转=2, 未愈=1, 其他=0
    '''
    new_data = list()
    for value in values:
        if value is None:
            new_data.append(None)
        else:
            value: str = value.strip()
            if value == "":
                new_data.append(None)
            elif value == "治愈":
                new_data.append(3)
            elif value == "好转":
                new_data.append(2)
            elif value == "未愈":
                new_data.append(1)
            else:
                new_data.append(0)
    
    return pl.Series(name=values.name, values=new_data, dtype=pl.Int32)


def 分娩方式(values: pl.Series):
    '''
    4 columns: 分娩方式_产钳术, 分娩方式_剖宫产, 分娩方式_臀部助娩, 分娩方式_自然分娩
    '''
    return values.to_dummies().get_columns()


def 血压(values: pl.Series):
    '''
    Divide the values into 2 columns: systolic blood pressure and diastolic blood pressure.
    '''
    systolic = list()
    diastolic = list()
    for value in values:
        if value is None:
            systolic.append(None)
            diastolic.append(None)
        else:
            value: str = value.strip()
            if value == "":
                systolic.append(None)
                diastolic.append(None)
            else:
                try:
                    splits = value.split("/")
                except:
                    systolic.append(None)
                    diastolic.append(None)
                else:
                    try:
                        systolic_value = int(splits[0])
                        diastolic_value = int(splits[1])
                    except:
                        systolic.append(None)
                        diastolic.append(None)
                    else:
                        systolic.append(systolic_value)
                        diastolic.append(diastolic_value)
    
    return [pl.Series(name="收缩压", values=systolic, dtype=pl.Int32), pl.Series(name="舒张压", values=diastolic, dtype=pl.Int32)]


def positive_negative(values: pl.Series):
    '''
    Binary encoding: 阴性=0, 阳性=1. Empty is also negative.
    '''
    new_data = list()
    for value in values:
        if value is None:
            new_data.append(0)
        else:
            value: str = value.strip()
            if value == "阳性":
                new_data.append(1)
            else:
                new_data.append(0)
    
    return pl.Series(name=values.name, values=new_data, dtype=pl.Int32)


def plus_symbols(values: pl.Series):
    '''
    `-` or 0 = 0 \\
    `+` or 1+ = 1 \\
    ++ or 2+ = 2 \\
    +++ or 3+ = 3 \\
    ++++ or 4+ = 4 \\
    None = None
    '''
    new_data = list()
    for value in values:
        if value is None:
            new_data.append(None)
        else:
            value: str = value.strip()
            if value == "":
                new_data.append(None)
            elif value in ['++++', '4+']:
                new_data.append(4)
            elif value in ['+++', '3+']:
                new_data.append(3)
            elif value in ['++', '2+']:
                new_data.append(2)
            elif value in ['+', '1+']:
                new_data.append(1)
            elif value in ['-', '0']:
                new_data.append(0)
            else:
                new_data.append(None)
                
    return pl.Series(name=values.name, values=new_data, dtype=pl.Int32)


#Rules mapping
RULES = {
    '主要治疗结果': 主要治疗结果,
    '分娩方式': 分娩方式,
    '血压': 血压,
    '抗甲状腺过氧化物酶抗体': positive_negative,
    '10.1-14周尿葡萄糖': plus_symbols,
    '14.1-19.6周尿葡萄糖': plus_symbols,
    '20-25周尿葡萄糖': plus_symbols,
    '25.1-27.6周尿葡萄糖': plus_symbols,
    '28-31周尿葡萄糖': plus_symbols,
    '31.1-33周尿葡萄糖': plus_symbols,
    '33.1-35周尿葡萄糖': plus_symbols,
    '35.1-36.3周尿葡萄糖': plus_symbols,
    '36.4-37.3周尿葡萄糖': plus_symbols,
    '37.4-38.3周尿葡萄糖': plus_symbols,
    '38.4-39.3周尿葡萄糖': plus_symbols,
    '39.4-40周尿葡萄糖': plus_symbols,
    '10.1-14周尿蛋白': plus_symbols,
    '14.1-19.6周尿蛋白': plus_symbols,
    '20-25周尿蛋白': plus_symbols,
    '25.1-27.6周尿蛋白': plus_symbols,
    '28-31周尿蛋白': plus_symbols,
    '31.1-33周尿蛋白': plus_symbols,
    '33.1-35周尿蛋白': plus_symbols,
    '35.1-36.3周尿蛋白': plus_symbols,
    '36.4-37.3周尿蛋白': plus_symbols,
    '37.4-38.3周尿蛋白': plus_symbols,
    '38.4-39.3周尿蛋白': plus_symbols,
    '39.4-40周尿蛋白': plus_symbols,
    '1-10周甲状腺过氧化物酶抗体': positive_negative,
    '10.1-20周甲状腺过氧化物酶抗体': positive_negative,
    '21-40周甲状腺过氧化物酶抗体': positive_negative,
    '10.1-14周尿酮体': plus_symbols,
    '14.1-19.6周尿酮体': plus_symbols,
    '20-25周尿酮体': plus_symbols,
    '25.1-27.6周尿酮体': plus_symbols,
    '28-31周尿酮体': plus_symbols,
    '31.1-33周尿酮体': plus_symbols,
    '33.1-35周尿酮体': plus_symbols,
    '35.1-36.3周尿酮体': plus_symbols,
    '36.4-37.3周尿酮体': plus_symbols,
    '37.4-38.3周尿酮体': plus_symbols,
    '38.4-39.3周尿酮体': plus_symbols,
    '39.4-40周尿酮体': plus_symbols,
}


#Process dataframe with rules
def process_dataframe(df: pl.DataFrame):
    processed_columns = list()
    for column in df.columns:
        if column in RULES.keys():
            resulting_column = RULES[column](df[column])
            if isinstance(resulting_column, list):
                processed_columns.extend(resulting_column)
            else:
                processed_columns.append(resulting_column)
        else:
            processed_columns.append(df[column])
            
    #Create and return new dataframe with processed columns
    return pl.DataFrame(processed_columns)


def check_data_numeric(df: pl.DataFrame) -> bool:
    # Print the shape of the DataFrame
    print(f"DataFrame shape: {df.shape}")
    
    # Initialize counters for integer and float columns
    int_count = 0
    float_count = 0
    
    # Iterate over the columns in the DataFrame
    for col in df.columns:
        if col == ID_COL:
            continue  # Skip the ID column
        dtype = df[col].dtype
        
        # Check for integer types
        if dtype in (pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            int_count += 1
        # Check for float types
        elif dtype in (pl.Float32, pl.Float64):
            float_count += 1
        else:
            print(f"Column {col} is neither integer nor float")
            return False  # Return False if any column is neither integer nor float

    # Print the number of integer and float columns
    print(f"Number of integer columns: {int_count}")
    print(f"Number of float columns: {float_count}")
    
    # Return True if all columns (except ID) are numeric
    return True


def load_dataframe(path: str=non_numeric_data_path):
    return pl.read_csv(path, infer_schema=False)


def save_dataframe(df: pl.DataFrame, path: str=non_numeric_data_v2_path):
    df.write_csv(path)
    print(f"Processed data saved to {path}")


def main():
    df = load_dataframe()
    processed_df = process_dataframe(df)
    if check_data_numeric(processed_df):
        save_dataframe(processed_df)


if __name__ == "__main__":
    main()
