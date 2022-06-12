import datetime
import multiprocessing as mp
import time
from argparse import ArgumentParser
import csv
import os


class PHRED_avgs_calculator:
    def __init__(self, input_file_path, output_file_path, block_size, max_processes=4):
        self.input_file_path = input_file_path
        self.block_size = block_size
        self.max_processes = max_processes
        self.output_file_path = output_file_path

    def translate_line(self, line):
        #print("translated line: {}".format(line))
        return [ord(char) - 33 for char in line]

    def calc_totals_from_lines(self, translated_lines):
        totals = []
        for line in translated_lines:
            for i, val in enumerate(line):
                if len(totals) < i + 1:
                    totals.append([val, 1])
                else:
                    totals[i][0] += val
                    totals[i][1] += 1
        return totals

    def translate_block(self, block, block_num, outputQ):
        translated_lines = []
        print("started block: {}".format(block_num))
        for num_line, line in enumerate(block):
            translated_lines.append(self.translate_line(line))
        outputQ.put(self.calc_totals_from_lines(translated_lines))
        print("finished block: {}".format(block_num))

    def find_longest_list_len(self, parent_list):
        longest_len = 0
        for list in parent_list:
            if longest_len < len(list):
                longest_len = len(list)
        return longest_len

    def calc_avg_from_column(self, column, blocks_totals, blocks_totals_len, outputQ):
        new_total = 0
        occurences = 0
        # TODO check if dont assume collunm length because of different read length
        for j in range(0, blocks_totals_len):
            try:
                new_total += blocks_totals[j][column][0]
                occurences += blocks_totals[j][column][1]
            except IndexError:
                print("caught a error but its harmless")
            continue
        outputQ.put([column, new_total / occurences])

    def calc_averages(self, blocks_totals):
        blocks_totals_len = len(blocks_totals)
        longest_read = self.find_longest_list_len(blocks_totals)
        averages = []
        processes = []
        outputQ = mp.Queue()
        for i in range(0, longest_read):
            while len(processes) >= int(self.max_processes):
                no_procces_closed = False
                for process in processes:
                    if not process.is_alive():
                        process.join()
                        processes.remove(process)
                    else:
                        no_procces_closed = True

                if no_procces_closed:
                    time.sleep(0.2)
            else:
                temP = mp.Process(target=self.calc_avg_from_column, args=(i, blocks_totals, blocks_totals_len, outputQ))
                processes.append(temP)
                temP.start()

        for p in processes:
            p.join()

        while not outputQ.empty():
            averages.append(outputQ.get())

        return averages

    def calc_averages_serial(self, blocks_totals):
        blocks_totals_len = len(blocks_totals)
        longest_read = self.find_longest_list_len(blocks_totals)
        averages = []
        outputQ = []
        for i in range(0, longest_read):
            outputQ.append(self.calc_avg_from_column(i, blocks_totals, blocks_totals_len, outputQ))
        return averages

    def order_averages(self, un_ordered_averages):
        ordered_averages = [0 for i in range(len(un_ordered_averages))]
        for avg in un_ordered_averages:
            ordered_averages[avg[0]] = avg[1]
        return ordered_averages

    def calculator(self):
        file = open(self.input_file_path, "r")

        block = []
        block_num = 1
        processes = []
        outputQ = mp.Queue()
        blocks_totals = []

        # TODO calc intermediated avg inside blocks
        for num_line, line in enumerate(file):
            if not (num_line + 1) % 4:
                if len(block) <= self.block_size:
                    block.append(line)
                else:
                    # make sure we dont start more than max_processes
                    while len(processes) >= int(self.max_processes):
                        no_procces_closed = False
                        for process in processes:
                            if not process.is_alive():
                                process.join()
                                processes.remove(process)
                            else:
                                no_procces_closed = True

                        if no_procces_closed:
                            time.sleep(0.2)
                    else:
                        temP = mp.Process(target=self.translate_block, args=(block, block_num, outputQ))
                        processes.append(temP)
                        temP.start()
                        block = []
                        block_num += 1
                        while not outputQ.empty():
                            blocks_totals.append(outputQ.get())

        # finish last block
        temP = mp.Process(target=self.translate_block, args=(block, block_num, outputQ))
        processes.append(temP)
        temP.start()

        file.close()
        print("done reading file")
        for p in processes:
            p.join()
        while not outputQ.empty():
            blocks_totals.append(outputQ.get())

        #print(blocks_totals)
        print("finished last block calculating averages now")
        un_ordered_averages = self.calc_averages(blocks_totals)
        ordered_averages = self.order_averages(un_ordered_averages)

        formated_averages = ""
        for i, val in enumerate(ordered_averages):
            formated_averages += "Base position: {} average: {}\n ".format(i, val)
        print("Calculated the following averages: \n {}".format(formated_averages))
        print("Wrote averages to: {}".format(self.output_file_path))
        if self.output_file_path:
            output_file = open(self.output_file_path, 'w')
            with output_file:
                writer = csv.writer(output_file)
                writer.writerows(un_ordered_averages)


def main():
    parser = ArgumentParser("usage example server:opdracht2.py -f /path/to/file.fastq -n 16 -w 10 -b 1000 -p 4223"
                            " --hosts 127.0.0.1  -s \n"
                            "usage example client:opdracht2.py -p 4223 -H 127.0.0.1 -c \n")
    parser.add_argument('input_file', metavar='N', type=str, nargs=1,
                        help='Input fastq file to be processed')
    parser.add_argument('-n', '--processes', required=False, default=200,
                        help="Number of processes")
    parser.add_argument('-b', '--block_size', required=False, default=100000,
                        help="Max number of lines a block can process")
    parser.add_argument('-o', '--output_file', required=False,
                        help="Path to output file", default="outputfile.csv")
    args = parser.parse_args()

    begin_time = datetime.datetime.now()
    calculator = PHRED_avgs_calculator(input_file_path=str(args.input_file[0]), output_file_path=args.output_file,
                                       block_size=int(args.block_size), max_processes=args.processes)
    calculator.calculator()
    print(datetime.datetime.now() - begin_time)


if __name__ == '__main__':
    main()
