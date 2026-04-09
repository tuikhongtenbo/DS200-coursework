-- Load du lieu goc
raw = LOAD 'C:/E old/DS200/DS200-coursework/Lab2/hotel-review.csv' USING PigStorage(';') AS (id: int, review: chararray, category: chararray, aspect: chararray, sentiment: chararray);

-- Chuyen thanh chu thuong
lowered = FOREACH raw GENERATE LOWER(review) AS review, category;

-- Loai bo ky tu dac biet, chi giu chu cai va khoang trang
cleaned = FOREACH lowered GENERATE
    REPLACE(review, '[^a-zA-ZÀ-ỹ\\s]', ' ') AS review,
    category;

-- Loc dong rong
cleaned = FILTER cleaned BY review IS NOT NULL AND review != '';

-- Tach tu
tokenized = FOREACH cleaned GENERATE
    FLATTEN(TOKENIZE(review)) AS word,
    category;

-- Loai bo tu rong
tokenized = FILTER tokenized BY word != '';

-- Load stopwords de loai bo
stopwords = LOAD 'C:/E old/DS200/DS200-coursework/Lab2/stopwords.txt' USING PigStorage() AS (word: chararray);

-- JOIN trai loai bo stopword
joined = JOIN tokenized BY word LEFT OUTER, stopwords BY word;

-- Chi giu tu khong phai stopword
filtered = FILTER joined BY stopwords::word IS NULL;

-- Chuan hoa
words = FOREACH filtered GENERATE
    tokenized::word AS word,
    tokenized::category AS category;

-- Group theo (category, word) va dem tan suat
word_group = GROUP words BY (category, word);
word_count = FOREACH word_group GENERATE
    FLATTEN(group) AS (category, word),
    COUNT(words) AS count;

-- Group theo category roi lay 5 tu co tan suat cao nhat
category_group = GROUP word_count BY category;
top5_words = FOREACH category_group {
    sorted = ORDER word_count BY count DESC;
    top5 = LIMIT sorted 5;
    GENERATE FLATTEN(top5);
};

DUMP top5_words;

-- LUU KET QUA
STORE top5_words INTO 'C:/E old/DS200/DS200-coursework/Lab2/output/Bai_5/top5_words' USING PigStorage('\t');