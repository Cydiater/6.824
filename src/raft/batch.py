import argparse
import subprocess

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--command', default = 'go test -race')
    parser.add_argument('--batch_size', type = int, default = 10)
    parser.add_argument('--prefix', default = 'rf')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    cmds = [args.command + ' > ' + args.prefix + str(i) + '.log' for i in range(args.batch_size)]
    processes = [subprocess.Popen(cmd, shell = True) for cmd in cmds]
    for process in processes:
        process.wait()
