
from gensim.models import word2vec

def main():

    num_features = 300    # Word vector dimensionality
    min_word_count = 10   # Minimum word count
    num_workers = 10       # Number of threads to run in parallel
    context = 10          # Context window size
    downsampling = 1e-3   # Downsample setting for frequent words
    sentences=[]

    with open("../../data/dblp/corpus.txt", encoding='utf8', mode='r') as fp:
        for line in fp.readlines():
            line=line.replace('\n','')
            sentences.append(line.split(" "))


    model = word2vec.Word2Vec(sentences, workers=num_workers, \
                              vector_size=num_features, min_count = min_word_count, \
                              window = context, sg = 0, sample = downsampling)
    model.init_sims(replace=True)
    # 保存模型，供日後使用
    model.save("paper_model")

    # 可以在加载模型之后使用另外的句子来进一步训练模型
    # model = gensim.models.Word2Vec.load('/tmp/mymodel')
    # model.train(more_sentences)

if __name__ == "__main__":
    main()