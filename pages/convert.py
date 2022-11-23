# Python program to read
# json file



def main():
    import json
  
    # Opening JSON file
    f = open('data.json')
  
    # returns JSON object as 
    # a dictionary
    data = json.load(f)

    print("DATA")
    print(data)
  
    # Iterating through the json
    # list
    #
    #print(i)


if __name__=="__main__":
    main()