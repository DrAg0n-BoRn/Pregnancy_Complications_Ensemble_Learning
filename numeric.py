## Fill with mean and mode values

import polars as pl
import os

numeric_data_path = os.path.join(os.getcwd(), "outputs", "raw_numeric_data.csv")
numeric_data_v2_path = os.path.join(os.getcwd(), "outputs", "processed_numeric_data.csv")

#Define completion rules
mean_columns = [
    "体重",
    "身高",
    '1小时葡萄糖',
    '2小时葡萄糖',
    '空腹葡萄糖',
    '25.1-33.6周γ-谷氨酰转肽酶',
    '34-37.6周γ-谷氨酰转肽酶',
    '1-22周乳酸脱氢酶',
    '22.1-32周乳酸脱氢酶',
    '32.1-40周乳酸脱氢酶',
    '1-10周促甲状腺素',
    '10.1-20周促甲状腺素',
    '21-40周促甲状腺素',
    '1-25周叶酸',
    '25.1-40周叶酸',
    '34-37.6周总胆固醇',
    '1-10周游离FT4',
    '10.1-20周游离FT4',
    '21-40周游离FT4',
    '13-25周谷丙转氨酶',
    '25.1-33.6周谷丙转氨酶',
    '34-37.6周谷丙转氨酶',
    '38-40周谷丙转氨酶',
    '13-25周谷草转氨酶',
    '25.1-33.6周谷草转氨酶',
    '34-37.6周谷草转氨酶',
    '38-40周谷草转氨酶',
    '34-37.6周载脂蛋白A1',
    '34-37.6周载脂蛋白B',
    '18.1-25w_双顶径',
    '25.1-32w_双顶径',
    '32.1-38w_双顶径',
    '18.1-25w_羊水深度',
    '25.1-32w_羊水深度',
    '32.1-38w_羊水深度',
    '18.1-25w_脐动脉S/D',
    '32.1-38w_脐动脉S/D',
    '38.1-40w_脐动脉S/D',
    '25.1-32w_脐动脉RI',
    '32.1-38w_脐动脉RI',
    '38.1-40w_脐动脉RI',
    '25.1-32w_脐动脉S/D',
    '18.1-25w_脐动脉RI',
    '1-11.6周糖化血红蛋白'
]

# median_columns = []

mode_columns = [
    "孕妇产次",
    "足月产次数",
    "早产次数",
    "流产次数",
    "人流次数",
]


def make_polars_expressions(df: pl.DataFrame):
    df_column_names = df.columns
    expressions = []
    # use mean value
    for column in mean_columns:
        if column not in df_column_names:
            raise ValueError(f"Column {column} (fill with mean value) not found in dataframe")
        expressions.append(pl.col(column).fill_null(pl.col(column).mean()).alias(column))
    # use the mode value
    for column in mode_columns:
        if column not in df_column_names:
            raise ValueError(f"Column {column} (fill with the mode value) not found in dataframe")
        mode_value = df[column].mode()[0]
        expressions.append(pl.col(column).fill_null(mode_value).alias(column))
                           
    return expressions


def fill_missing_values(df: pl.DataFrame):
    expressions = make_polars_expressions(df)
    
    df_filled = df.with_columns(expressions)
    
    #Verify the total number of missing values in the entire DataFrame
    null_counts_df = df_filled.null_count()
    null_counts_dict = null_counts_df.to_dict(as_series=False)
    total_nulls = sum(v[0] for v in null_counts_dict.values())
    print(f"Total missing values in the DataFrame: {total_nulls}")
    
    return df_filled


def load_dataframe(path: str=numeric_data_path):
    return pl.read_csv(path, infer_schema=True)


def save_dataframe(df: pl.DataFrame, path: str=numeric_data_v2_path):
    df.write_csv(path)
    

def main():
    df = load_dataframe()
    df_filled = fill_missing_values(df)
    save_dataframe(df_filled)

if "__main__" == __name__:
    main()
    