import csv
import datetime
import multiprocessing as mp
import os
import queue
import time
from argparse import ArgumentParser
from multiprocessing.managers import BaseManager

class PhredAvgsCalculator:
    def __init__(self, input_file_path, output_file_path, block_size, chunk_positions, max_processes=4):
        self.input_file_path = input_file_path
        self.block_size = block_size
        self.max_processes = max_processes
        self.output_file_path = output_file_path
        self.chunk_positions = chunk_positions

    @staticmethod
    def translate_line(line):
        return [ord(char) - 33 for char in line]

    @staticmethod
    def calc_totals_from_lines(translated_lines):
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
        # print("started block: {}".format(block_num))
        for num_line, line in enumerate(block):
            translated_lines.append(self.translate_line(line))
        outputQ.put(self.calc_totals_from_lines(translated_lines))
        # print("finished block: {}".format(block_num))

    @staticmethod
    def find_longest_list_len(parent_list):
        longest_len = 0
        for child_list in parent_list:
            if longest_len < len(child_list):
                longest_len = len(child_list)
        return longest_len

    @staticmethod
    def calc_avg_from_column(column, blocks_totals, blocks_totals_len, outputQ):
        new_total = 0
        occurences = 0
        # TODO check if dont assume collunm length because of different read length
        for j in range(0, blocks_totals_len):
            try:
                new_total += blocks_totals[j][column][0]
                occurences += blocks_totals[j][column][1]
            except IndexError:
                # TODO fix this
                print("ERROR i cant handle files with differing read lengths! TODO")
                pass
        outputQ.put([column, new_total / occurences])

    def merge_block_totals(self, block_totals):
        longest_read = self.find_longest_list_len(block_totals)
        merged_block_totals = [[0, 0] for i in range(0, longest_read)]
        for line in block_totals:
            for i, totals in enumerate(line):
                merged_block_totals[i][0] += totals[0]
                merged_block_totals[i][1] += totals[1]
        return merged_block_totals

    def calc_averages(self, blocks_totals):
        blocks_totals_len = len(blocks_totals)
        longest_read = self.find_longest_list_len(blocks_totals)
        averages = []
        processes = []
        output_q = mp.Queue()
        recent_print = False
        for i in range(0, longest_read):
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
                    # don't waste cpu on running this while loop
                    if not (int(time.time()) % 1):
                        if not recent_print:
                            print(
                                "more than {} processes running waiting until finnished".format(self.max_processes))
                            recent_print = True
                    else:
                        recent_print = False

            tem_p = mp.Process(target=self.calc_avg_from_column, args=(i, blocks_totals, blocks_totals_len, output_q))
            processes.append(tem_p)
            tem_p.start()

        for p in processes:
            p.join()

        while not output_q.empty():
            averages.append(output_q.get())

        return averages

    def calc_averages_serial(self, blocks_totals):
        blocks_totals_len = len(blocks_totals)
        longest_read = self.find_longest_list_len(blocks_totals)
        averages = []
        outputQ = []

        for i in range(0, longest_read):
            outputQ.append(self.calc_avg_from_column(i, blocks_totals, blocks_totals_len, outputQ))
        return averages

    @staticmethod
    def order_averages(un_ordered_averages):
        ordered_averages = [0 for i in range(len(un_ordered_averages))]
        for avg in un_ordered_averages:
            ordered_averages[avg[0]] = avg[1]
        return ordered_averages

    def calculate(self):
        block = []
        block_num = 1
        processes = []
        output_q = mp.Queue()
        blocks_totals = []

        file = open(self.input_file_path, "r")
        num_line = 0
        line = file.readline()

        file.seek(self.chunk_positions[0])
        recent_print = False
        while line:
            if file.tell() == self.chunk_positions[1]:
                break
            else:
                if len(block) <= self.block_size:
                    if not (num_line % 4):
                        block.append(line)
                else:
                    # make sure we dont start more than max_processes
                    while len(processes) >= int(self.max_processes):
                        for process in processes:
                            if not process.is_alive():
                                process.join()
                                processes.remove(process)

                        # don't waste cpu on running this while loop
                        if not (int(time.time()) % 1):
                            if not recent_print:
                                print(
                                    "more than {} processes running waiting until finnished".format(self.max_processes))
                                recent_print = True
                        else:
                            recent_print = False
                    else:
                        tem_p = mp.Process(target=self.translate_block, args=(block, block_num, output_q))
                        processes.append(tem_p)
                        tem_p.start()
                        block = []
                        block_num += 1
                        while not output_q.empty():
                            blocks_totals.append(output_q.get())
                num_line += 1
                line = file.readline()
        file.close()

        # finish last block
        tem_p = mp.Process(target=self.translate_block, args=(block, block_num, output_q))
        processes.append(tem_p)
        tem_p.start()

        file.close()
        print("done reading file")

        for p in processes:
            p.join()
        while not output_q.empty():
            blocks_totals.append(output_q.get())
            # TODO might be able to parallel
        return self.merge_block_totals(blocks_totals)


class GridCalculator:
    # TODO create server and client class
    def __init__(self, max_processes, port, hosts, block_size, input_file_path="", output_file_path="", n_chunks=10):
        self.POISONPILL = "MEMENTOMORI"
        self.ERROR = "DOH"
        self.IP = hosts[0]
        self.PORTNUM = port
        self.AUTHKEY = b'whathasitgotinitspocketsesss?'
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.max_processes = max_processes
        self.block_size = block_size
        self.hosts = hosts
        self.n_chunks = n_chunks

    @staticmethod
    def make_server_manager(port, authkey):
        """ Create a manager for the server, listening on the given port.
            Return a manager object with get_job_q and get_result_q methods.
        """
        job_q = queue.Queue()
        result_q = queue.Queue()

        # This is based on the examples in the official docs of multself.IProcessing.
        # get_{job|result}_q return synchronized proxies for the actual Queue
        # objects.
        class QueueManager(BaseManager):
            pass

        QueueManager.register('get_job_q', callable=lambda: job_q)
        QueueManager.register('get_result_q', callable=lambda: result_q)

        manager = QueueManager(address=('', port), authkey=authkey)
        manager.start()
        print('Server started at port %s' % port)
        return manager

    @staticmethod
    def make_client_manager(ip, port, authkey):
        """ Create a manager for a client. This manager connects to a server on the
            given address and exposes the get_job_q and get_result_q methods for
            accessing the shared queues from the server.
            Return a manager object.
        """

        class ServerQueueManager(BaseManager):
            pass

        ServerQueueManager.register('get_job_q')
        ServerQueueManager.register('get_result_q')

        manager = ServerQueueManager(address=(ip, port), authkey=authkey)
        manager.connect()

        print('Client connected to %s:%s' % (ip, port))
        return manager

    @staticmethod
    def capitalize(word):
        """Capitalizes the word you pass in and returns it"""
        return word.upper()

    @staticmethod
    def calculate_chunk(d):
        input_file_path, output_file_path, block_size, max_processes, file_positions = d
        # TODO maybe not do sepperate class anymore
        calculator = PhredAvgsCalculator(input_file_path=str(input_file_path), output_file_path=str(output_file_path),
                                         block_size=int(block_size), chunk_positions=file_positions,
                                         max_processes=int(max_processes))
        return calculator.calculate()

    def peon(self, job_q, result_q):
        recent_print = False
        my_name = mp.current_process().name
        while True:
            try:
                job = job_q.get_nowait()
                if job == self.POISONPILL:
                    job_q.put(self.POISONPILL)
                    print("Aaaaaaargh", my_name)
                    return
                else:
                    try:
                        result = job['fn'](job['arg'])
                        print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                        result_q.put({'job': job, 'result': result})
                    except NameError:
                        print("Can't find yer fun Bob!")
                        result_q.put({'job': job, 'result': self.ERROR})

            except queue.Empty:
                time.sleep(0.01)
                if not (time.time() % 0.2):
                    if not recent_print:
                        print("sleepytime for", my_name)
                        recent_print = True
                else:
                    recent_print = False

    # skipping this because calculate_block will do multi thread
    def run_workers(self, job_q, result_q, num_processes):
        processes = []
        for p in range(num_processes):
            temP = mp.Process(target=self.peon, args=(job_q, result_q))
            processes.append(temP)
            temP.start()
        print("Started %s workers!" % len(processes))
        for temP in processes:
            temP.join()

    def run_client(self, num_processes):
        manager = self.make_client_manager(self.IP, self.PORTNUM, self.AUTHKEY)
        job_q = manager.get_job_q()
        result_q = manager.get_result_q()
        # do peon instead of run_workers because calculate_block will do multi thread
        self.peon(job_q, result_q)  # , num_processes)

    @staticmethod
    def split_file(n_chunks, input_file_path):
        file_size = os.path.getsize(input_file_path)
        chunk_size = int(file_size / n_chunks)
        file = open(input_file_path, "r")
        chunk_positions = []
        start_pos_chunk = 0
        for i in range(1, n_chunks):

            file.seek(i * chunk_size)
            while True:
                current_pos = file.tell()
                line = file.readline()
                next_pos = file.tell()
                file.seek(current_pos - 1)
                # if current line start with + and next byte is a \n to make sure it is indeed the start of the line
                if line.startswith("@") and file.read(1) == "\n":
                    file.readline()
                    nex_line = file.readline()
                    if not nex_line.startswith("@"):
                        # next line is at nex_pos is a full line and a header
                        chunk_positions.append([start_pos_chunk, current_pos])
                        start_pos_chunk = current_pos
                        file.seek(next_pos)
                        break
                else:
                    # go next line
                    file.seek(next_pos)
        print("split file in {} chunks with position: {}".format(len(chunk_positions), chunk_positions))
        file.close()
        return chunk_positions

    @staticmethod
    def format_averages(ordered_averages):
        formatted_averages = ""
        for i, val in enumerate(ordered_averages):
            formatted_averages += "Base position: {} average: {}\n ".format(i, val)
        return formatted_averages

    @staticmethod
    def write_averages_to_file(output_file_path, ordered_averages):
        if output_file_path:
            output_file = open(output_file_path, 'w')
            with output_file:
                writer = csv.writer(output_file)
                writer.writerow(["#read_position", "average"])
                writer.writerows([[i, val] for i, val in enumerate(ordered_averages)])
        else:
            return 1

    def collect_and_kill(self, shared_result_q, shared_job_q, chunk_positions):
        results = []
        while True:
            try:
                result = shared_result_q.get_nowait()
                results.append(result)
                if len(results) == len(chunk_positions):
                    print("Got all results!")
                    break
            except queue.Empty:
                time.sleep(0.001)
                continue
        # Tell the client process no more data will be forthcoming
        print("Time to kill some peons!")
        shared_job_q.put(self.POISONPILL)
        blocks_totals = []
        for item in results:
            blocks_totals.append(item["result"])
        return blocks_totals

    def send_chunk_positions_to_que(self, chunk_positions, shared_job_q, fn):
        for chunk in chunk_positions:
            # TODO dont need output_file_path
            data = [self.input_file_path, self.output_file_path, self.block_size, self.max_processes, chunk]
            shared_job_q.put({'fn': fn, 'arg': data})

    def runserver(self, fn, data):
        # keep track of total run time
        begin_time = datetime.datetime.now()
        # Start a shared manager server and access its queues
        manager = self.make_server_manager(self.PORTNUM, b'whathasitgotinitspocketsesss?')
        shared_job_q = manager.get_job_q()
        shared_result_q = manager.get_result_q()
        chunk_positions = self.split_file(self.n_chunks + 1, self.input_file_path)
        print("Sending data!")
        self.send_chunk_positions_to_que(chunk_positions, shared_job_q, fn)
        print("waiting for results")
        blocks_totals = self.collect_and_kill(shared_result_q, shared_job_q, chunk_positions)
        # Sleep a bit before shutting down the server - to give clients time to
        # realize the job queue is empty and exit in an orderly way.
        time.sleep(0.5)
        print("Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaand we're done for the server manager!")
        manager.shutdown()

        print("finished last block calculating averages now")
        # TODO chunk_positions should maybe not be in init / maybe merge the two objects
        calculator = PhredAvgsCalculator(input_file_path=str(self.input_file_path),
                                         output_file_path=str(self.output_file_path),
                                         block_size=0, chunk_positions=0,
                                         max_processes=int(self.max_processes))
        # TODO floating points error are adding up
        # TODO spread over workers??
        un_ordered_averages = calculator.calc_averages(blocks_totals)
        ordered_averages = calculator.order_averages(un_ordered_averages)

        print("Calculated the following averages: \n {}".format(self.format_averages(ordered_averages)))
        print("Wrote averages to: {}".format(self.output_file_path))
        print("I have been running for: {}".format(datetime.datetime.now() - begin_time))
        self.write_averages_to_file(self.output_file_path, ordered_averages)


def run_type_factory(is_server, is_client, args):
    if is_server:
        grid = GridCalculator(max_processes=args.processes, port=args.port,
                              hosts=args.hosts, block_size=args.block_size, input_file_path=args.input_file,
                              output_file_path=args.output_file, n_chunks=args.n_chunks)
        # TODO calculate_chunk needs data but we dont have any so remove []
        server = mp.Process(target=grid.runserver, args=(grid.calculate_chunk, []))
        server.start()
        server.join()
    elif is_client:
        grid = GridCalculator(max_processes=args.processes, port=args.port,
                              hosts=args.hosts, block_size=args.block_size)
        # time.sleep(1)
        client = mp.Process(target=grid.run_client, args=(args.processes,))
        client.start()
        client.join()
    else:
        assert (args.client or args.server or (args.client and args.server)), "I dont know what i am. " \
                                                                              "please add a --server/-s " \
                                                                              "or --client/-c flag"


def main():
    parser = ArgumentParser("TODO")
    parser.add_argument('-f', '--input_file', metavar='N', type=str,
                        help='Input fastq file to be processed')
    parser.add_argument('-n', '--processes', required=False, default=16, type=int,
                        help="Number of processes per client")
    parser.add_argument('-w', '--n_chunks', required=False, default=30, type=int,
                        help="Number of chunks to split the file at")
    parser.add_argument('-b', '--block_size', required=False, default=100000,
                        help="Max number of lines a block can process")
    parser.add_argument('-o', '--output_file', required=False,
                        help="Path to output file", default="outputfile.csv")
    parser.add_argument('-H', '--hosts', required=False, nargs="+",
                        help="Path to output file", default="127.0.0.1")
    parser.add_argument('-p', '--port', required=False, type=int,
                        help="The port used for client server communication", default="4223")
    # TODO double check
    parser.add_argument('-c', '--client', required=False, dest='client', action='store_true',
                        help="Make this computer a client", default=False)
    parser.add_argument('-s', '--server', required=False, dest='server', action='store_true',
                        help="Make this computer a server", default=False)
    args = parser.parse_args()

    run_type_factory(is_server=args.server, is_client=args.client, args=args)


if __name__ == '__main__':
    main()
