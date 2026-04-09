-- 1. Load du lieu
data = LOAD 'C:/E old/DS200/DS200-coursework/Lab2/hotel-review.csv' USING PigStorage(';') AS (id: int, review: chararray, category: chararray, aspect: chararray, sentiment: chararray);

-- 2. Hien thi 10 sample dau tien
samples = LIMIT data 10;
DUMP samples;

-- 3. Chuyen tat ca ky tu ve chu thuong
data = FOREACH data GENERATE
    LOWER(review) AS review,
    category,
    aspect,
    sentiment;

-- 4. Loai bo cac dau dac biet va so, chi giu lai chu cai va khoang trang
data = FOREACH data GENERATE
    REPLACE(review, '[^a-zA-ZÀ-ỹ\\s]', ' ') AS review,
    category,
    aspect,
    sentiment;

-- 5. Loai bo dong null va rong
data = FILTER data BY review IS NOT NULL AND review != '';

-- 6. Tach cau thanh cac tu (theo khoang trang)
words = FOREACH data GENERATE
    FLATTEN(TOKENIZE(review)) AS word,
    category,
    aspect,
    sentiment;

-- 7. Load danh sach stopword
stopwords = LOAD 'C:/E old/DS200/DS200-coursework/Lab2/stopwords.txt' USING PigStorage() AS (word: chararray);

-- 8. JOIN trai de loai bo stopword (LEFT OUTER + loc NULL)
joined_words = JOIN words BY word LEFT OUTER, stopwords BY word;

-- 9. Chi giu lai nhung tu khong phai stopword
words = FILTER joined_words BY stopwords::word IS NULL;

-- 10. Chuan hoa ten cot ket qua
words = FOREACH words GENERATE
    words::word AS word,
    words::category AS category,
    words::aspect AS aspect,
    words::sentiment AS sentiment;

-- 11. Hien thi 20 ket qua dau tien 
result = LIMIT words 20;
DUMP result;

-- 12. Luu ket qua ra thu muc output
STORE words INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_1' USING PigStorage('\t');