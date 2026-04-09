-- 1. Load du lieu tu ket qua Bai 1
words = LOAD 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_1' USING PigStorage('\t') AS (word: chararray, category: chararray, aspect: chararray, sentiment: chararray);

-- =============================================
-- PHAN 1: Tan so xuat hien cua cac tu
-- =============================================
word_count = FOREACH words GENERATE word;
word_freq = GROUP word_count BY word;
freq_count = FOREACH word_freq GENERATE group AS word, COUNT(word_count) AS count;

-- Loc nhung tu xuat hien tren 500 lan
frequent_words = FILTER freq_count BY count > 500;
frequent_words_ordered = ORDER frequent_words BY count DESC;
frequent_20 = LIMIT frequent_words_ordered 20;
DUMP frequent_20;

-- =============================================
-- PHAN 2: So binh luan theo tung category
-- =============================================
category_data = LOAD 'C:/E old/DS200/DS200-coursework/Lab2/hotel-review.csv' USING PigStorage(';') AS (id: int, review: chararray, category: chararray, aspect: chararray, sentiment: chararray);

-- Group theo category va dem so binh luan
category_group = GROUP category_data BY category;
category_count = FOREACH category_group GENERATE group AS category, COUNT(category_data) AS count;

category_count_ordered = ORDER category_count BY count DESC;
category_20 = LIMIT category_count_ordered 20;
DUMP category_20;

-- =============================================
-- PHAN 3: So binh luan theo tung aspect
-- =============================================
aspect_group = GROUP category_data BY aspect;
aspect_count = FOREACH aspect_group GENERATE group AS aspect, COUNT(category_data) AS count;

aspect_count_ordered = ORDER aspect_count BY count DESC;
aspect_20 = LIMIT aspect_count_ordered 20;
DUMP aspect_20;

-- LUU KET QUA
STORE frequent_words_ordered INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_2/frequent_words' USING PigStorage('\t');
STORE category_count_ordered INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_2/category_count' USING PigStorage('\t');
STORE aspect_count_ordered INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_2/aspect_count' USING PigStorage('\t');