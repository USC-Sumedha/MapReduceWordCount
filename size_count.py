import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    
    long = 'long'     #10 or more letters in a word
    medium = 'medium' #5 to 9 letters in a word
    small = 'small'   #2 to 4 letters in a word
    tiny = 'tiny'     #1 letter words

    # initialize a dictionary to store the word length and counts
    subMapCount = {long:0,medium:0,small:0,tiny:0}

    # iterate the split records and find the count of corresponsing lengths
    for w in words :
      if len(w) >= 10:
          subMapCount[long] += 1
      elif len(w) in range(5,10):
          subMapCount[medium] += 1
      elif len(w) in range(2,5):
          subMapCount[small] += 1
      elif len(w) == 1:   
          subMapCount[tiny] += 1
    
    # store the lengths as tuple value and pass on to reducer
    # key : document_Id as key
    # value : (wordlength, wordcount)
    for w in subMapCount:
        print w,subMapCount[w]
        mr.emit_intermediate(record[0],(w,subMapCount[w]))
	
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = {'long':0,'medium':0,'small':0,'tiny':0}

    #iterate over each documents summarized values and add the total count
    # v : ('type of word',count)
    for v in list_of_values:
                if v[0] in ['long','small','medium','tiny']:
                     total[v[0]] = total[v[0]] + v[1]
    finalOutput = []
    for k,v in total.iteritems():
        finalOutput.append((k,v))

    mr.emit((key, finalOutput))  

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
