-- Load du lieu tu Bai 1
words = LOAD 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_1' USING PigStorage('\t') AS (word: chararray, category: chararray, aspect: chararray, sentiment: chararray);

-- =============================================
-- PHAN 1: Aspect nhan nhieu danh gia tieu cuc nhat
-- =============================================
negative_words = FILTER words BY sentiment == 'negative';

negative_by_aspect = GROUP negative_words BY aspect;
negative_count = FOREACH negative_by_aspect GENERATE
    group AS aspect,
    COUNT(negative_words) AS count;

negative_most = ORDER negative_count BY count DESC;
negative_top = LIMIT negative_most 5;
DUMP negative_top;

-- =============================================
-- PHAN 2: Aspect nhan nhieu danh gia tich cuc nhat
-- =============================================
positive_words = FILTER words BY sentiment == 'positive';

positive_by_aspect = GROUP positive_words BY aspect;
positive_count = FOREACH positive_by_aspect GENERATE
    group AS aspect,
    COUNT(positive_words) AS count;

positive_most = ORDER positive_count BY count DESC;
positive_top = LIMIT positive_most 5;
DUMP positive_top;

-- LUU KET QUA
STORE negative_top INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_3/negative_by_aspect' USING PigStorage('\t');
STORE positive_top INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_3/positive_by_aspect' USING PigStorage('\t');