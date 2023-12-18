import argparse, os

def checkOutput(path):
    for filename in os.listdir(path):
        if filename.endswith(".output"):
            prefix = filename.split('.')[0]
            with open(os.path.join(path, filename)) as f_output, open(os.path.join(path, prefix+'.config')) as f_config:
                print(f"{f_output.name} - {f_config.name}")
                output_lines = f_output.readlines()
                config_lines = f_config.readlines()
                config = config_lines[0].split()
                nb_agreement = int(config[0])
                nb_max = int(config[1])
                cnt = 0
                for out_line, conf_line in zip(output_lines, config_lines[1:]):
                    out_set = set(out_line.split())
                    conf_set = set(conf_line.split())
                    assert conf_set.issubset(out_set)
                    cnt += 1
                assert cnt == nb_agreement
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, type=str, dest="path", help="Path to the directory containing the files")

    results = parser.parse_args()
    checkOutput(results.path)