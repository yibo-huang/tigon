import argparse
import pwr


def set_uncore(socket: int, val: int):
    cpus = pwr.get_cpus()
    for (idx, cpu) in enumerate(cpus):
        if idx == socket:
            cpu.uncore_max_freq = val
            cpu.uncore_min_freq = val
            cpu.commit()


def print_current_uncore_freq():
    cpus = pwr.get_cpus()
    for (idx, cpu) in enumerate(cpus):
        print(f"cpu id: {cpu.cpu_id}")
        print(f"    uncore hw min/max: {cpu.uncore_hw_min},{cpu.uncore_hw_max}")
        print(f"    uncore min/max: {cpu.uncore_min_freq},{cpu.uncore_max_freq}")
        print(f"    uncore freq: {cpu.uncore_freq}")


def print_freq(args: argparse.Namespace):
    print_current_uncore_freq()


def set_freq(args: argparse.Namespace):
    set_uncore(args.socket, args.freq)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    parser_print = subparsers.add_parser("print", help="print uncore frequency on intel machine")
    parser_print.set_defaults(func=print_freq)

    parser_set = subparsers.add_parser("set", help="set uncore frequency on intel machine")
    parser_set.add_argument("--freq", type=int, required=True,
                            help="frequency to set to")
    parser_set.add_argument("--socket", type=int, required=True,
                            help="socket to modify")
    parser_set.set_defaults(func=set_freq)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()

