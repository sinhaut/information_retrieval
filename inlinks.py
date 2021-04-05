from collections import defaultdict

def get_inlinks_from_outlinks():
    inlinks = defaultdict(set)
    outlinks = []
    with open('outlinks_k.csv', 'r') as outlinks_life:
        outlinks = outlinks_life.readlines()
    
   # i = 0
    for links in outlinks:
        all_links = links.split(', ')
        inlink = all_links[0]
        for out in all_links[1:]:
            inlinks[out].add(inlink)
        #import pdb; pdb.set_trace()
        
    
    with open('inlinks_k.csv', 'w') as inlinks_file:
        for out, ins in inlinks.items():
            s = str(out) + ", " + ", ".join(set(ins)) + "\n"
            #import pdb; pdb.set_trace()
            inlinks_file.write(s)
    print('finished')

if __name__ == "__main__":
    get_inlinks_from_outlinks()
