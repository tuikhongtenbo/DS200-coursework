-- Load du lieu tu Bai 1
words = LOAD 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_1' USING PigStorage('\t') AS (word: chararray, category: chararray, aspect: chararray, sentiment: chararray);

-- =============================================
-- PHAN 1: 5 tu tich cuc nhat theo tung category
-- =============================================
positive_words = FILTER words BY sentiment == 'positive';

positive_group = GROUP positive_words BY (category, word);
positive_word_count = FOREACH positive_group GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(positive_words) AS count;

-- Group theo category roi order theo count
positive_cat_group = GROUP positive_word_count BY category;
positive_top5 = FOREACH positive_cat_group {
    sorted = ORDER positive_word_count BY count DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5);
};

DUMP positive_top5;

-- =============================================
-- PHAN 2: 5 tu tieu cuc nhat theo tung category
-- =============================================
negative_words = FILTER words BY sentiment == 'negative';

negative_group = GROUP negative_words BY (category, word);
negative_word_count = FOREACH negative_group GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(negative_words) AS count;

negative_cat_group = GROUP negative_word_count BY category;
negative_top5 = FOREACH negative_cat_group {
    sorted = ORDER negative_word_count BY count DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5);
};

DUMP negative_top5;

-- LUU KET QUA
STORE positive_top5 INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_4/positive_words' USING PigStorage('\t');
STORE negative_top5 INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_4/negative_words' USING PigStorage('\t');