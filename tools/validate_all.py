import argparse, os

def checkMessages(path, nb_proc, nb_msg):
    results = [{} for i in range(nb_proc)]
    for i in range(nb_proc):
        results[i]['b'] = []
        for j in range(nb_proc):
            key = 'd '+str(j+1)
            results[i][key] = []
    proc_cnt = 0
    for filename in os.listdir(path):
        if filename.endswith(".output"):
            print(filename)
            with open(os.path.join(path, filename)) as f:
                lines = f.readlines()
                for line in lines :
                    tokens = line.split()
                    if tokens[0] == 'b':
                        msg = int(tokens[1])
                        results[proc_cnt]['b'].append(msg)
                    elif tokens[0] == 'd':
                        sender = int(tokens[1])
                        msg = int(tokens[2])
                        results[proc_cnt]['d '+str(sender)].append(msg)
            for j in results[proc_cnt]:
                print(j)
                results[proc_cnt][j].sort()
                assert results[proc_cnt][j] == list(range(1, nb_msg+1))
            proc_cnt += 1
    

    
                


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, type=str, dest="path", help="Path to the directory containing the output files")
    parser.add_argument("--nb_proc", required=True, type=int, dest="nb_proc", help="Number of processes")
    parser.add_argument("--nb_msg", required=True, type=int, dest="nb_msg", help="Number of messages")

    results = parser.parse_args()
    
    checkMessages(results.path, results.nb_proc, results.nb_msg)

