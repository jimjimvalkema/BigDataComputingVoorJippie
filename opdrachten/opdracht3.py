import csv
import datetime
import multiprocessing as mp
import os
import queue
import time
from argparse import ArgumentParser
from multiprocessing.managers import BaseManager
import shlex
import subprocess
# TODO numpy
import socket


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
    def __init__(self, max_processes, port, hosts, block_size, input_file_path="", output_file_path="", n_chunks=10,
                 port_range=[4223, 6000]):
        self.POISONPILL = "MEMENTOMORI"
        self.ERROR = "DOH"
        self.server_ip = hosts[0]
        self.PORTNUM = port
        self.AUTHKEY = b'passwordlole2?'
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.max_processes = max_processes
        self.block_size = block_size
        self.hosts = hosts
        self.n_chunks = n_chunks
        self.script_location = os.path.realpath(__file__)
        self.port_range = port_range

    @staticmethod
    def l_to_str(var_as_l):
        return str(var_as_l).replace(",", " ").replace("'", "")[1:-1]

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
    def check_clients(managers):
        results = []
        assert len(results) < len(managers), "spawning clients failed. one more more didn't spawn or doesn't have a " \
                                             "result que"
        for ip in managers.keys():
            try:
                # result_que = managers[ip]["manager"].get_result_q().get_nowait()
                result = managers[ip]["manager"].get_result_q().get_nowait()
                if not result == "im running!":
                    raise Exception("All clients spawned but one more more had a malformed result que"
                                    "\n expected: \'im running!\' got \'{}\'".format(result))
                else:
                    print("Client at {} with port {} spawned successfully!".format(ip, managers[ip]["port"]))
            except queue.Empty:
                raise Exception("All clients spawned but {} had a empty result que. Is it stuck and "
                                "still running?".format(ip))
                continue

    @staticmethod
    def test_port(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return not s.connect_ex(('localhost', port)) == 0

    def get_free_port(self, ports):
        for i, port in enumerate(ports):
            if self.test_port(port):
                return port, ports[i:]
        raise Exception("Couldn't find a free port. From the a list of {} ports. Are those open?".format(len(ports)))

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
                        print("Peon {} Workwork on {}!".format(my_name, job['arg']))
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

    # TODO plugin PHRED_avg_calculator
    # skipping this because calculate_block will do multi thread
    def run_workers(self, job_q, result_q, num_processes):
        processes = []
        for p in range(num_processes):
            tem_p = mp.Process(target=self.peon, args=(job_q, result_q))
            processes.append(tem_p)
            tem_p.start()
        print("Started %s workers!" % len(processes))
        for tem_p in processes:
            tem_p.join()

    def run_client(self, num_processes):
        manager = self.make_client_manager(self.server_ip, self.PORTNUM, self.AUTHKEY)
        job_q = manager.get_job_q()
        result_q = manager.get_result_q()
        result_q.put("im running!")
        self.peon(job_q, result_q)  # , num_processes)

    def spawn_clients_at(self, hosts, port_range, authkey=b'passwordlole2?'):
        ports = list(range(port_range[0], port_range[1]))
        managers = {}
        # TODO move assert to main
        assert len(ports) > len(hosts), \
            "Port range too small! Please provide more ports! ex: --portrange 4500 " + str(len(hosts) + 5)
        for i, client_ip in enumerate(hosts):
            port, ports = self.get_free_port(ports)
            manager = self.make_server_manager(port, authkey)
            print("created a manager ", manager)
            managers[client_ip] = {"manager": manager, "port": port,
                                   "hosts": hosts, "block_size": self.block_size}
            # TODO test ips to check if they fail
            print("spawning client {} at port {}".format(client_ip, port))
            command_line = "ssh {} python3 {} --port {} --hosts {} --block_size {} -c".format(
                client_ip, os.path.realpath(__file__), port, self.l_to_str(self.hosts), self.block_size)
            subprocess.Popen(
                shlex.split(command_line))  # to silence, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            print("ran this command to spawn a client : " + command_line)
        time.sleep(1)
        self.check_clients(managers)
        return managers

    @staticmethod
    def collect_results(chunk_positions, result_qs):
        results = []
        last_result = 0
        while len(results) < len(chunk_positions):
            for result_q in result_qs:
                try:
                    result = result_q.get_nowait()
                    results.append(result)
                except queue.Empty:
                    time.sleep(0.1)
                    if not last_result == len(results):
                        print("Finished {} blocks of the {}".format(len(results), len(chunk_positions)))
                    last_result = len(results)
                    continue
        return results

    @staticmethod
    def shutdown_all_managers(client_managers):
        for ip in client_managers:
            client_managers[ip]["manager"].shutdown()
        time.sleep(0.1)
        return 0

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

    def collect_and_kill(self, chunk_positions, result_qs, client_managers):
        results = self.collect_results(chunk_positions, result_qs)
        print("Got all results!")
        # Tell the client process no more data will be forthcoming
        print("Time to kill some peons!")
        for ip in client_managers:
            client_managers[ip]["manager"].get_job_q().put(self.POISONPILL)
        blocks_totals = []
        for item in results:
            blocks_totals.append(item["result"])
        self.shutdown_all_managers(client_managers)
        return blocks_totals

    def send_data_to_clients(self, chunk_positions, client_managers, job_qs, fn):
        for i, chunk in enumerate(chunk_positions):
            print("adding chunk: ", chunk)
            data = [self.input_file_path, self.output_file_path, self.block_size, self.max_processes, chunk]
            job_qs[i % len(client_managers)].put({'fn': fn, 'arg': data})
        return job_qs

    def runserver(self, fn):

        # keep track of total run time
        begin_time = datetime.datetime.now()

        print("spawning clients!")
        client_managers = self.spawn_clients_at(self.hosts[1:], self.port_range, b'passwordlole2?')

        # TODO chunks size arg
        # TODO check if not empty
        chunk_positions = self.split_file(self.n_chunks + 1, self.input_file_path)

        print("creating ques!")
        job_qs = [client_managers[ip]["manager"].get_job_q() for ip in client_managers.keys()]
        result_qs = [client_managers[ip]["manager"].get_result_q() for ip in client_managers.keys()]

        print("sending data")
        job_qs = self.send_data_to_clients(chunk_positions, client_managers, job_qs, fn)

        print("waiting for results")
        blocks_totals = self.collect_and_kill(chunk_positions, result_qs, client_managers)

        print("Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaand we're done for the server manager!")
        self.shutdown_all_managers(client_managers)

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
        print("starting as a server")
        args.portrange = [int(i) for i in args.portrange]
        grid = GridCalculator(max_processes=args.processes, port=args.port,
                              hosts=args.hosts, block_size=args.block_size, input_file_path=args.input_file,
                              output_file_path=args.output_file, n_chunks=args.chunks, port_range=args.portrange)
        server = mp.Process(target=grid.runserver, args=(grid.calculate_chunk,))
        server.start()
        server.join()
    elif is_client:
        print("starting as a client")
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
    # TODO notes:
    # heartbeat is extra 09:36/16:11 jun4
    # index error op line 69 geen meme. Alleen als reads ongelijke lengte hebben
    parser = ArgumentParser("TODO")
    parser.add_argument('-f', '--input_file', metavar='N', type=str,
                        help='Input fastq file to be processed')
    parser.add_argument('-n', '--processes', required=False, default=16, type=int,
                        help="Number of processes per client")
    parser.add_argument('-w', '--chunks', required=False, default=30, type=int,
                        help="Number of chunks to split the file at")
    parser.add_argument('-b', '--block_size', required=False, default=100000,
                        help="Max number of lines a block can process")
    parser.add_argument('-o', '--output_file', required=False,
                        help="Path to output file", default="outputfile.csv")
    parser.add_argument('-H', '--hosts', required=False, nargs="+",
                        help="Path to output file", default="127.0.0.1")
    parser.add_argument('-p', '--port', required=False, type=int,
                        help="The port used for client server communication", default="4223")
    parser.add_argument('-c', '--client', required=False, dest='client', action='store_true',
                        help="Make this computer a client", default=False)
    parser.add_argument('-s', '--server', required=False, dest='server', action='store_true',
                        help="Make this computer a server", default=False)
    parser.add_argument('-R', '--portrange', required=False, nargs=2,
                        help="The range of available ports. Needs to be bigger then the number of hosts",
                        default=["4500", "4700"])
    args = parser.parse_args()

    run_type_factory(is_server=args.server, is_client=args.client, args=args)


if __name__ == '__main__':
    main()
