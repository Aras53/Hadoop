from mrjob.job import MRJob
from mrjob.step import MRStep

class SortRating (MRJob):
    MRJob.SORT_VALUES = True

    def steps(self):
        ratings = [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_sum_ratings),
            MRStep(mapper=self.mapper_make_sum_key,
                   reducer=self.reducer_output_results)
        ]
        return ratings

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_sum_ratings(self, key, values):
        yield key, sum(values)

    def mapper_make_sum_key(self, key, values):
        yield None, ("%07.02f" % values, key)

    def reducer_output_results(self, key, values):
        for i in values:
            yield i[1], i[0]

if __name__ == '__main__':
    SortRating.run()
