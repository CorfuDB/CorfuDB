import matplotlib.pyplot as plt
import numpy as np
import os as os
import time
from scipy import stats

input_path = "/Users/vshriya/Documents/CorfuDB/CorfuDB/test/client-1528755755604.log"
# input_path = "/Users/vshriya/Documents/CorfuDB/CorfuDB/test/mylog.log"
# input_directory = "/Users/vshriya/Documents/CorfuDB/CorfuDB/test/maithem_test"
output_path = "results/shriya/jul10/"
points = []  # all data points, aka each entry in log files
all_ops = []  # all operations mentioned in log files


### Helper Functions
def setup():
    global points, all_ops

    # open file and populate global vars
    f = open(input_path)
    for point in f.readlines():
        points.append(point)
        if get_event_name(point) not in all_ops:
            all_ops.append(get_event_name(point))

    # sort points by start time
    points.sort(key=lambda x: get_time_stamp(x))


def setup_all():
    global points, all_ops

    # open file and populate global vars
    for subdir, dirs, files in os.walk(input_directory):
        for file in files:
            # print os.path.join(subdir, file)
            filepath = subdir + os.sep + file
            if "log" not in filepath:
                continue
            print(filepath)

            f = open(filepath)
            for point in f.readlines():
                if "cacheLoad" not in point:  # filtering out cacheLoads
                    points.append(point)
                    if get_event_name(point) not in all_ops:
                        all_ops.append(get_event_name(point))


    # sort points by start time
    print(len(points))
    points.sort(key=lambda x: get_time_stamp(x))


def get_time_stamp(entry):
    return int(entry.split()[0])


def get_thread_ID(entry):
    return int(entry.split()[1])


def get_table_ID(entry):
    return entry.split("[id]")[1].split()[0]


def get_event_name(entry):
    return entry.split()[3].split()[0]


def get_duration(entry):
    if "[dur]" not in entry:
        return 0
    return int(entry.split("[dur]")[1].split()[0])


def get_method_name(entry):
    return entry.split("[method]")[1].split()[0]


def nano_to_milli(time):
    return time / 1000000.0


### Analysis
def count_active_threads():
    '''
    Answers the query: how many threads are active per millisecond?
    Creates a scatter plot
    '''
    print 1
    # Count active threads per ms
    counts = {}  # ms interval --> set of active thread IDs
    for point in points:
        if int(nano_to_milli(get_time_stamp(point))) in counts:
            counts[int(nano_to_milli(get_time_stamp(point)))].add(get_thread_ID(point))
        else:
            counts[int(nano_to_milli(get_time_stamp(point)))] = set([get_thread_ID(point)])

    # Create scatter plot
    x = []
    y = []
    for key in counts.keys():
        x += [key - min(counts.keys())]
        y += [len(counts[key])]

    plt.scatter(x, y, alpha=0.5)
    plt.title("Number of Active Threads vs. Time Elapsed")
    plt.xlabel("Time Elapsed (ms)")
    plt.ylabel("Number of Active Threads")
    plt.savefig(output_path + "count_active_threads.png", bbox_inches='tight')
    plt.clf()


def count_seq_calls():
    '''
    Answers the query: how many sequencer calls are made per millisecond?
    Creates a scatter plot
    '''
    print 2
    # Count sequencer calls per ms
    counts = {}  # ms interval --> counts of seq calls
    for point in points:
        if get_event_name(point) == "Seq":
            if int(nano_to_milli(get_time_stamp(point))) in counts:
                counts[int(nano_to_milli(get_time_stamp(point)))] += 1
            else:
                counts[int(nano_to_milli(get_time_stamp(point)))] = 1

    # Create scatter plot
    x = []
    y = []
    for key in counts.keys():
        x += [key - min(counts.keys())]
        y += [counts[key]]

    plt.scatter(x, y)
    plt.title("Number of Seq Calls vs. Time Elapsed")
    plt.xlabel("Time Elapsed (ms)")
    plt.ylabel("Number of Seq Calls")
    plt.savefig(output_path + "count_seq_calls.png", bbox_inches='tight')
    plt.clf()


def count_ops_per_tx():
    '''
    Answers the query: how many operations are performed per transaction?
    Creates a histogram and a box plot
    '''
    print 3
    # Count number of ops in every transaction
    counts = {}  # thread num --> count of ops (in one tx in that thread)
    num_ops = []  # list of number of ops taken by each tx
    for point in points:
        if get_event_name(point) == "TXBegin":
            counts[get_thread_ID(point)] = 0
        elif get_event_name(point) == "TXEnd":
            num_ops += [counts[get_thread_ID(point)]]
            counts[get_thread_ID(point)] = 0
        elif get_thread_ID(point) in counts:
            counts[get_thread_ID(point)] += 1

    # Create box plot
    bp_dict = plt.boxplot(num_ops, vert=0, sym='k.')

    # Label median
    x, y = bp_dict['medians'][0].get_xydata()[1]  # top of median line
    plt.text(x, y + 0.01, '%.1f' % x, horizontalalignment='center')  # draw above, centered

    # Plot/label mean
    plt.scatter(np.mean(num_ops), [1], color='red')
    plt.text(np.mean(num_ops), (1 + 0.01), '%.1f' % np.mean(num_ops), horizontalalignment='center', fontsize=12)

    plt.xscale('log')
    plt.savefig(output_path + "count_ops_per_tx_boxplot.png", bbox_inches='tight')
    plt.clf()

    # Count frequency of each number of ops
    num_num_ops = {}  # number --> number of times that number appears
    for n in num_ops:
        if n not in num_num_ops:
            num_num_ops[n] = 1
        else:
            num_num_ops[n] += 1
    x = num_num_ops.keys()
    y = num_num_ops.values()

    # Create histogram
    data = []
    for num in num_num_ops:
        for i in range(num_num_ops[num]):
            data += [num]
    weights = np.ones_like(data) / float(len(data))
    plt.hist(data, weights=weights, facecolor='#a7d656')

    plt.xlabel('Num Ops')
    plt.ylabel('Frequency')
    plt.title('Num Ops per Transaction')
    plt.grid(True)
    plt.savefig(output_path + "count_ops_per_tx_histogram.png", bbox_inches='tight')
    plt.clf()


def count_avg_time_per_tx():
    '''
    Answers the query: how much time, on average, does a transaction take?
    Creates a box plot
    '''
    print 4
    # Count number of ops in every transaction
    counts = {}  # thread num --> count of ops (in one tx in that thread)
    times = []  # list of number of ops taken by each tx
    for point in points:
        if get_event_name(point) == "TXBegin":
            counts[get_thread_ID(point)] = get_time_stamp(point)
        elif get_event_name(point) == "TXEnd":
            times += [nano_to_milli(get_time_stamp(point) - counts[get_thread_ID(point)])]
            counts[get_thread_ID(point)] = 0

    # Create boxplot
    bp_dict = plt.boxplot(times, vert=0, sym='k.')

    # Label median
    x, y = bp_dict['medians'][0].get_xydata()[1]  # top of median line
    plt.text(x, y + 0.01, '%.1f' % x, horizontalalignment='center')  # draw above, centered

    # Plot/label mean
    plt.scatter(np.mean(times), [1], color='red')
    plt.text(np.mean(times), (1 + 0.01), '%.1f' % np.mean(times), horizontalalignment='center', fontsize=12)

    plt.xscale('log')
    plt.savefig(output_path + "count_avg_time_per_tx_boxplot.png", bbox_inches='tight')
    plt.clf()


def count_table_ops(access_ops, mutate_ops):  # access_ops, mutate_ops = list of strings with names of ops
    '''
    Answers query: how many read/write table operations are performed on each table?
    Creates a horizontal stacked bar chart
    '''
    print 5
    # Count number of access/mutate ops for each table
    counts = {}  # table_id --> [count of ops]
    labels = access_ops + mutate_ops
    for point in points:
        if "[method]" in point:
            if get_table_ID(point) not in counts:
                counts[get_table_ID(point)] = [0 for _ in range(len(labels))]
            if get_method_name(point) in labels:
                counts[get_table_ID(point)][labels.index(get_method_name(point))] += 1

    # Process counts dict to make suitable for bar chart
    tables = counts.keys()
    data = []
    for op in labels:
        data += [np.array([counts[table_id][labels.index(op)] for table_id in counts.keys()])]
    data = np.array(data)

    # Create bar chart
    y_pos = np.arange(len(tables))
    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(111)

    colors = plt.cm.Set2(np.array([(i - len(counts) / 16) / (len(counts) * 2.0) for i in range(0, 2 * len(counts), 2)]))

    patch_handles = []
    left = np.zeros(len(tables))  # left alignment of data starts at zero
    for i, d in enumerate(data):
        patch_handles.append(ax.barh(y_pos, d,
                                     color=colors[i % len(colors)], align='center',
                                     left=left, label=labels[i]))
        # accumulate the left-hand offsets
        left += d

    # go through all of the bar segments and annotate
    for j in xrange(len(patch_handles)):
        for i, patch in enumerate(patch_handles[j].get_children()):
            bl = patch.get_xy()
            x = 0.5 * patch.get_width() + bl[0]
            y = 0.5 * patch.get_height() + bl[1]
            ax.text(x, y, data[j, i], ha='center')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(tables)
    ax.set_xlabel('Op Count')
    ax.set_ylabel('Table ID')
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

    plt.savefig(output_path + "count_table_ops.png", bbox_inches='tight')
    plt.clf()


def count_table_ops_per_tx(access_ops, mutate_ops):  # access_ops, mutate_ops = list of strings with names of ops
    '''
    Answers query: how many read/write table operations are performed, on average, per transaction?
    Creates a horizontal stacked bar chart
    '''
    print 6
    # Count number of access/mutate ops for each table within each transaction
    counts = {}  # thread num --> {table_id --> [count of ops]}
    table_counts = {}  # table_id --> list of lists, where elem i = list of counts of op i
    labels = access_ops + mutate_ops
    for point in points:
        if get_event_name(point) == "TXBegin":
            counts[get_thread_ID(point)] = {}
        elif get_event_name(point) == "TXEnd":
            if get_thread_ID(point) in counts:
                for table in counts[get_thread_ID(point)].keys():
                    if table not in table_counts:
                        table_counts[table] = [ counts[get_thread_ID(point)][table][i] for i in range(len(labels)) ]
                    else:
                        for i in range(len(labels)):
                            table_counts[table][i] += counts[get_thread_ID(point)][table][i]
                del counts[get_thread_ID(point)]
        elif "[method]" in point and "(tx)" in point:
            if get_thread_ID(point) in counts and get_table_ID(point) not in counts[get_thread_ID(point)]:
                counts[get_thread_ID(point)][get_table_ID(point)] = [0 for _ in range(len(labels))]
            if get_method_name(point) in labels:
                counts[get_thread_ID(point)][get_table_ID(point)][labels.index(get_method_name(point))] += 1

    # Process counts dict to make suitable for bar chart
    tables = table_counts.keys()
    data = []
    for op in labels:
        data += [np.array([ np.mean(table_counts[table_id][labels.index(op)]) for table_id in table_counts.keys()]) ]
    data = np.array(data)

    # Create bar chart
    y_pos = np.arange(len(tables))
    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(111)

    colors = plt.cm.Set2(
        np.array([(i - len(table_counts) / 16) / (len(table_counts) * 2.0) for i in range(0, 2 * len(table_counts), 2)]))

    patch_handles = []
    left = np.zeros(len(tables))  # left alignment of data starts at zero
    for i, d in enumerate(data):
        patch_handles.append(ax.barh(y_pos, d,
                                     color=colors[i % len(colors)], align='center',
                                     left=left, label=labels[i]))
        # accumulate the left-hand offsets
        left += d

    # go through all of the bar segments and annotate
    for j in xrange(len(patch_handles)):
        for i, patch in enumerate(patch_handles[j].get_children()):
            bl = patch.get_xy()
            x = 0.5 * patch.get_width() + bl[0]
            y = 0.5 * patch.get_height() + bl[1]
            ax.text(x, y, data[j, i], ha='center')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(tables)
    ax.set_xlabel('Op Count')
    ax.set_ylabel('Table ID')
    plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

    plt.savefig(output_path + "count_table_ops_per_tx.png", bbox_inches='tight')
    plt.clf()


def count_id_table_ops_per_tx(access_ops, mutate_ops, table_id):
    '''
    Answers query: how many read/write table operations are performed on a given table per transaction?
    Creates a double bar graph
    '''
    print 7
    counts_access = {}  # thread num --> count of access ops
    counts_mutate = {}  # thread num --> count of mutate ops
    num_access_ops = []  # contains total number of access ops in each tx
    num_mutate_ops = []  # contains total number of mutate ops in each tx
    for point in points:
        if get_event_name(point) == "TXBegin":
            counts_access[get_thread_ID(point)] = 0
            counts_mutate[get_thread_ID(point)] = 0
        elif get_event_name(point) == "TXEnd":
            num_access_ops += [counts_access[get_thread_ID(point)]]
            num_mutate_ops += [counts_mutate[get_thread_ID(point)]]
            counts_access[get_thread_ID(point)] = 0
            counts_mutate[get_thread_ID(point)] = 0
        elif table_id in point:
            for op in access_ops:
                if "[method] " + op in point and get_thread_ID(point) in counts_access:
                    counts_access[get_thread_ID(point)] += 1
                    break
            for op in mutate_ops:
                if "[method] " + op in point and get_thread_ID(point) in counts_mutate:
                    counts_mutate[get_thread_ID(point)] += 1
                    break

    # Count frequency of each number of ops
    num_num_access_ops = {}  # number --> number of times that number appears
    for n in num_access_ops:
        if n not in num_num_access_ops:
            num_num_access_ops[n] = 1
        else:
            num_num_access_ops[n] += 1

    num_num_mutate_ops = {}  # number --> number of times that number appears
    for n in num_mutate_ops:
        if n not in num_num_mutate_ops:
            num_num_mutate_ops[n] = 1
        else:
            num_num_mutate_ops[n] += 1

    # Create double bar chart
    x = np.array(list(set(num_num_access_ops.keys()).union(set(num_num_mutate_ops.keys()))))
    y_access = []
    y_mutate = []
    for item in x:
        if item not in num_num_access_ops:
            num_num_access_ops[item] = 0
        if item not in num_num_mutate_ops:
            num_num_mutate_ops[item] = 0
        y_access += [num_num_access_ops[item]]
        y_mutate += [num_num_mutate_ops[item]]
    width = 0.27
    plt.bar(x, y_access, width, color="blue", label="accessOps")
    plt.bar(x + width, y_mutate, width, color="red", label="mutateOps")
    plt.legend(loc=1)
    plt.title("Num Ops per Transaction")
    plt.xlabel("Num Ops")
    plt.ylabel("Num Txs")
    plt.savefig(output_path + "count_id_table_ops_per_tx.png", bbox_inches='tight')
    plt.clf()


def count_all_ops():
    '''
    Answers the query: how many times was each operation called w.r.t. the total number of calls?
    Creates a pie chart
    '''
    print 8
    # Count number of times each operation was called
    counts = {}  # op name --> count
    for point in points:
        if get_event_name(point) not in counts:
            counts[get_event_name(point)] = 1
        else:
            counts[get_event_name(point)] += 1

    # Sort values and labels in ascending order by value
    values = []
    labels = []
    other = 0
    total_count = sum(counts.values())
    for key, value in sorted(counts.iteritems(), key=lambda (k, v): (v, k)):
        if value / (1.0 * total_count) < 0.015:
            other += value
            continue
        labels += [key + "\n(" + str(value) + ")"]  # label = name of op + num of occurrences [string]
        values += [value]  # value = time taken by op
    labels += ["Other" + "\n(" + str(other) + ")"]
    values += [other]

    # Create pie chart
    colors = plt.cm.Set2(np.array([(i - len(counts) / 16) / (len(counts) * 2.0) for i in range(0, 2 * len(counts), 2)]))

    fig = plt.figure(figsize=[10, 10])
    ax = fig.add_subplot(111)
    pie_wedge_collection = ax.pie(values, colors=colors, labels=labels, pctdistance=0.7, radius=3, autopct='%1.1f%%')
    plt.axis('equal')
    for pie_wedge in pie_wedge_collection[0]:
        pie_wedge.set_edgecolor('white')
    fig.text(.5, .05, "Total Count: " + str(total_count) + " ops", ha='center', fontweight="bold")

    plt.savefig(output_path + "count_all_ops.png", bbox_inches='tight')
    plt.clf()


def count_all_methods():
    '''
    Answers the query: how many times was each method called w.r.t. the total number of calls?
    Creates a pie chart
    '''
    print 9
    # Count number of times each operation was called
    counts = {}  # op name --> count
    for point in points:
        if "[method]" in point:
            if get_method_name(point) not in counts:
                counts[get_method_name(point)] = 1
            else:
                counts[get_method_name(point)] += 1

    # Sort values and labels in ascending order by value
    values = []
    labels = []
    other = 0
    total_count = sum(counts.values())
    for key, value in sorted(counts.iteritems(), key=lambda (k, v): (v, k)):
        if value / (1.0 * total_count) < 0.015:
            print(value)
            other += value
            continue
        labels += [key + "\n(" + str(value) + ")"]  # label = name of op + num of occurrences [string]
        values += [value]  # value = time taken by op
    labels += ["Other" + "\n(" + str(other) + ")"]
    values += [other]

    # Create pie chart
    colors = plt.cm.Set2(np.array([(i - len(counts) / 16) / (len(counts) * 2.0) for i in range(0, 2 * len(counts), 2)]))

    fig = plt.figure(figsize=[10, 10])
    ax = fig.add_subplot(111)
    pie_wedge_collection = ax.pie(values, colors=colors, labels=labels, pctdistance=0.7, radius=3, autopct='%1.1f%%')
    plt.axis('equal')
    for pie_wedge in pie_wedge_collection[0]:
        pie_wedge.set_edgecolor('white')
    fig.text(.5, .05, "Total Count: " + str(total_count) + " ops", ha='center', fontweight="bold")

    plt.savefig(output_path + "count_all_methods.png", bbox_inches='tight')
    plt.clf()


def count_all_methods_tx():
    '''
    Answers the query: how many times was each method called in a transaction w.r.t. the total number of calls?
    Creates a pie chart
    '''
    print 10
    # Count number of times each operation was called
    counts = {}  # op name --> count
    for point in points:
        if "(tx)" in point and "[method]" in point:
            if get_method_name(point) not in counts:
                counts[get_method_name(point)] = 1
            else:
                counts[get_method_name(point)] += 1

    # Sort values and labels in ascending order by value
    values = []
    labels = []
    other = 0
    total_count = sum(counts.values())
    for key, value in sorted(counts.iteritems(), key=lambda (k, v): (v, k)):
        if value / (1.0 * total_count) < 0.015:
            print(value)
            other += value
            continue
        labels += [key + "\n(" + str(value) + ")"]  # label = name of op + num of occurrences [string]
        values += [value]  # value = time taken by op
    labels += ["Other" + "\n(" + str(other) + ")"]
    values += [other]

    # Create pie chart
    colors = plt.cm.Set2(np.array([(i - len(counts) / 16) / (len(counts) * 2.0) for i in range(0, 2 * len(counts), 2)]))

    fig = plt.figure(figsize=[10, 10])
    ax = fig.add_subplot(111)
    pie_wedge_collection = ax.pie(values, colors=colors, labels=labels, pctdistance=0.7, radius=3, autopct='%1.1f%%')
    plt.axis('equal')
    for pie_wedge in pie_wedge_collection[0]:
        pie_wedge.set_edgecolor('white')
    fig.text(.5, .05, "Total Count: " + str(total_count) + " ops", ha='center', fontweight="bold")

    plt.savefig(output_path + "count_all_methods_tx.png", bbox_inches='tight')
    plt.clf()


def count_all_methods_by_id(table_id):
    '''
    Answers the query: how many times was each method called for a given table w.r.t. the total number of calls?
    Creates a pie chart
    '''
    print 11
    # Count number of times each operation was called
    counts = {}  # op name --> count
    for point in points:
        if table_id in point and "[method]" in point:
            if get_method_name(point) not in counts:
                counts[get_method_name(point)] = 1
            else:
                counts[get_method_name(point)] += 1

    # Sort values and labels in ascending order by value
    values = []
    labels = []
    other = 0
    total_count = sum(counts.values())
    for key, value in sorted(counts.iteritems(), key=lambda (k, v): (v, k)):
        if value / (1.0 * total_count) < 0.015:
            print(value)
            other += value
            continue
        labels += [key + "\n(" + str(value) + ")"]  # label = name of op + num of occurrences [string]
        values += [value]  # value = time taken by op
    labels += ["Other" + "\n(" + str(other) + ")"]
    values += [other]

    # Create pie chart
    colors = plt.cm.Set2(np.array([(i - len(counts) / 16) / (len(counts) * 2.0) for i in range(0, 2 * len(counts), 2)]))

    fig = plt.figure(figsize=[10, 10])
    ax = fig.add_subplot(111)
    pie_wedge_collection = ax.pie(values, colors=colors, labels=labels, pctdistance=0.7, radius=3, autopct='%1.1f%%')
    plt.axis('equal')
    for pie_wedge in pie_wedge_collection[0]:
        pie_wedge.set_edgecolor('white')
    fig.text(.5, .05, "Total Count: " + str(total_count) + " ops", ha='center', fontweight="bold")

    plt.savefig(output_path + "count_all_methods_by_id.png", bbox_inches='tight')
    plt.clf()


def create_trimmed_bar_graph(counts_dict, trim_constant, output_file):
    '''
    A helper function that calculates trim_constant% trimmed mean and outputs a bar graph representing
    this data to location output_file.
    '''
    trimmed_counts = {}
    # Average out time for each op
    for op in counts_dict:  # counts: op name --> trimmed avg time taken per call of op
        trimmed_counts[op] = stats.trim_mean(counts_dict[op], trim_constant)

    # Sort values and labels in ascending order by value
    values = []
    labels = []
    for key, value in sorted(trimmed_counts.iteritems(), key=lambda (k, v): (v, k)):
        labels += [key + "\n" + str(value) + " ms"]  # label = name of op + time taken by op (ms) [string]
        values += [value]  # value = time taken by op

    # Color Scheme
    colors = plt.cm.Set2(
        np.array([(i - len(labels) / 16) / (len(labels) * 2.0) for i in range(0, 2 * len(labels), 2)]))

    # Create bar graph
    plt.rcdefaults()
    fig, ax = plt.subplots()

    labels = [l.split("\n")[0] for l in labels]
    y_pos = np.arange(len(labels))

    r = ax.barh(y_pos, values, align='center', color=colors, ecolor='black')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel('Time (ms)')
    ax.set_title('Trimmed Average Time per Operation')

    for rect in r:
        width = rect.get_width()
        plt.text(rect.get_width() + 2, rect.get_y() + 0.5 * rect.get_height(),
                 '%f' % width, ha='center', va='center')

    plt.savefig(output_file, bbox_inches='tight')
    plt.clf()


def count_all_ops_time():
    '''
    Answers the query: how much time did a single call of each operation take on average?
    Creates a box plot and bar charts of medians, means, and trimmed means
    '''
    print 12
    # Count total time taken by each op
    counts = {}  # op name --> list of durations (ms)
    for point in points:
        if get_event_name(point) not in counts:
            counts[get_event_name(point)] = [nano_to_milli(get_duration(point))]
        else:
            counts[get_event_name(point)] += [nano_to_milli(get_duration(point))]

    # Create bar graph of trimmed avg time for each op
    create_trimmed_bar_graph(counts, 0.1, output_path + "count_all_ops_time_bar_10.png")
    create_trimmed_bar_graph(counts, 0.2, output_path + "count_all_ops_time_bar_20.png")
    create_trimmed_bar_graph(counts, 0.3, output_path + "count_all_ops_time_bar_30.png")

    # Create box plot with entire distribution
    fig = plt.figure()
    plt.hold = True
    boxes = []
    yticks = []
    for key, value in sorted(counts.iteritems(), key=lambda (k, v): -1 * np.mean(v)):
        boxes.append([counts[key]])
        yticks += [str(key) + " (median: " + str(np.median(counts[key])) + ")"]
    boxplot_info = plt.boxplot(boxes, vert=0, sym='k.')

    means = [np.mean(x) for x in boxes]
    plt.scatter(means, np.arange(1, len(counts.keys()) + 1), color='red')
    plt.yticks(np.arange(1, len(counts.keys()) + 1), yticks)
    plt.xscale('log')
    plt.xlim(left=-1)
    plt.xlabel("Time (ms)")
    plt.title("Time per Operation")

    plt.savefig(output_path + "count_all_ops_time_boxplot.png", bbox_inches='tight')
    plt.clf()

    # Average out time for each op
    total_time = 0
    for op in counts:  # counts: op name --> avg time taken per call of op
        counts[op] = sum(counts[op]) / (len(counts[op]) * 1.0)
        total_time += counts[op]

    # Sort values and labels in ascending order by value
    values = []
    labels = []
    for key, value in sorted(counts.iteritems(), key=lambda (k, v): (v, k)):
        labels += [key + "\n" + str(value) + " ms"]  # label = name of op + time taken by op (ms) [string]
        values += [value]  # value = time taken by op

    # Color Scheme
    colors = plt.cm.Set2(np.array([(i - len(labels) / 16) / (len(labels) * 2.0) for i in range(0, 2*len(labels), 2)]))

    # Create bar graph for mean
    plt.rcdefaults()
    fig, ax = plt.subplots()

    labels = [l.split("\n")[0] for l in labels]
    y_pos = np.arange(len(labels))

    r = ax.barh(y_pos, values, align='center', color=colors, ecolor='black')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel('Time (ms)')
    ax.set_title('Average Time per Operation')

    for rect in r:
        width = rect.get_width()
        plt.text(rect.get_width() + 2, rect.get_y() + 0.5 * rect.get_height(),
                 '%f' % width, ha='center', va='center')

    plt.savefig(output_path + "count_all_ops_time_mean_bar.png", bbox_inches='tight')
    plt.clf()


def count_all_ops_time_by_tx():
    '''
    Answers the query: how much time did each transactional operation take on average across all transactions
    w.r.t. the total runtime of all transactions?
    Creates a pie chart and two bar charts
    '''
    print 13
    # Count total time taken by each op w/i each tx
    # NOT CORRECT for same reason as above!
    txs = []  # list of lists, txs[i] = np.array of how much time each op takes in tx i
    in_tx = {}  # thread # --> (bool) currently in tx
    counts = {}  # thread # --> {op name --> list of durations (ms)}
    for point in points:
        if get_event_name(point) == "TXBegin":
            in_tx[get_thread_ID(point)] = True

        # only consider ops that are part of a tx
        if get_thread_ID(point) in in_tx and in_tx[get_thread_ID(point)]:
            if get_thread_ID(point) not in counts:
                counts[get_thread_ID(point)] = {}

            if get_event_name(point) not in counts[get_thread_ID(point)]:
                counts[get_thread_ID(point)][get_event_name(point)] = [nano_to_milli(get_duration(point))]
            else:
                counts[get_thread_ID(point)][get_event_name(point)] += [nano_to_milli(get_duration(point))]

        if get_event_name(point) == "TXEnd":
            # append info in counts to txs
            in_tx[get_thread_ID(point)] = False
            tx = []
            for i in range(len(all_ops)):
                if all_ops[i] in counts[get_thread_ID(point)]:
                    tx[i] = counts[get_thread_ID(point)][all_ops[i]]
                else:
                    tx[i] = []
            txs += [tx]
            # clear time_count for that thread
            del counts[get_thread_ID(point)]

    tx_mtx = np.array([tx for tx in txs])
    tx_mtx_T = tx_mtx.T  # aggregates counts s.t. each op has its own list

    avg_vals = []  # list of avg time taken by each op
    for op in tx_mtx_T:
        avg_vals += [np.average(op)]

    # create pie chart
    plt.pie(avg_vals, labels=all_ops, colors=['gold', 'yellowgreen', 'lightcoral', 'lightskyblue'], autopct='%1.1f%%')
    plt.axis('equal')
    plt.savefig(output_path + "count_all_ops_time_by_tx.png", bbox_inches='tight')
    plt.clf()


def count_reads():
    '''
    Answers the query: how many reads (both tx and non-tx) are done per millisecond?
    Creates a scatter plot, with both tx reads and non-tx reads
    '''
    print 14
    # Count tx reads
    counts_tx = {}  # ms interval --> count of tx reads
    for point in points:
        if get_event_name(point) == "access(tx)":
            if int(nano_to_milli(get_time_stamp(point))) in counts_tx:
                counts_tx[int(nano_to_milli(get_time_stamp(point)))] += 1
            else:
                counts_tx[int(nano_to_milli(get_time_stamp(point)))] = 1

    # Count non-tx reads
    counts_non_tx = {}  # ms interval --> count of non-tx reads
    for point in points:
        if get_event_name(point) == "access(nonTx)":
            if int(nano_to_milli(get_time_stamp(point))) in counts_non_tx:
                counts_non_tx[int(nano_to_milli(get_time_stamp(point)))] += 1
            else:
                counts_non_tx[int(nano_to_milli(get_time_stamp(point)))] = 1

    # Create juxtaposed scatter plot
    x_tx = []
    y_tx = []
    for key in counts_tx.keys():
        x_tx += [key - min(counts_tx.keys())]
        y_tx += [counts_tx[key]]
    plt.scatter(x_tx, y_tx, color='green', label="tx reads")

    x_non_tx = []
    y_non_tx = []
    for key in counts_non_tx.keys():
        x_non_tx += [key - min(counts_non_tx.keys())]
        y_non_tx += [counts_non_tx[key]]
    plt.scatter(x_non_tx, y_non_tx, label="non-tx reads")

    plt.title("Tx and Non-Tx Reads vs. Time")
    plt.xlabel("Time Elapsed (ms)")
    plt.ylabel("Number of Reads")
    plt.legend(loc=1)
    plt.savefig(output_path + "count_reads.png", bbox_inches='tight')
    plt.clf()


def count_access_lock_ops():
    '''
    Answers query: how much time do accessLock/accessLockOps operations take for each table?
    Creates a horizontal stacked bar chart
    '''
    print 15
    # Count number of access/mutate ops for each table
    counts = {}  # table_id --> list of times
    for point in points:
        if "accessLock" in point:
            if get_table_ID(point) not in counts:
                counts[get_table_ID(point)] = []
            counts[get_table_ID(point)] += [nano_to_milli(get_duration(point))]

    # Create boxplot
    fig = plt.figure()
    plt.hold = True
    boxes = []
    yticks = []
    for key, value in counts.iteritems():
        boxes.append([counts[key]])
        yticks.append(key)
    boxplot_info = plt.boxplot(boxes, vert=0, sym='k.')
    print(boxplot_info)
    print(boxplot_info['fliers'])
    print(len(boxplot_info['fliers']))

    means = [np.mean(x) for x in boxes]
    plt.scatter(means, np.arange(1, len(counts.keys()) + 1), color='red')

    for median in boxplot_info['medians']:
        # Label median
        x, y = median.get_xydata()[1]  # top of median line
        plt.text(x, y + 0.01, '%.5f' % x, horizontalalignment='center')  # draw above, centered

    # # Plot/label mean
    # plt.scatter(np.mean(times), [1], color='red')
    # plt.text(np.mean(times), (1 + 0.01), '%.1f' % np.mean(times), horizontalalignment='center', fontsize=12)

    plt.yticks(np.arange(1, len(counts.keys()) + 1), yticks)
    plt.xscale('log')
    plt.xlim(left=-1)
    plt.xlabel("Time (ms)")
    plt.ylabel("Table ID")
    plt.title("Time per Access Lock Operation")

    plt.savefig(output_path + "count_access_lock_ops.png", bbox_inches='tight')
    plt.clf()


### Display Results
start_time = time.time()

# setup_all()
setup()
# count_active_threads()
# count_seq_calls()
# count_ops_per_tx()
# count_avg_time_per_tx()
count_table_ops(["containsKey"], ["put"])
count_table_ops_per_tx(["containsKey"], ["put"])
# count_id_table_ops_per_tx(["containsKey"], ["put"], "7c4f2940-7893-3334-a6cb-7a87bf045c0d")
count_all_ops()
# count_all_methods()
# count_all_methods_tx()
# count_all_methods_by_id("7c4f2940-7893-3334-a6cb-7a87bf045c0d")
# count_all_ops_time()
# count_all_ops_time_by_tx()
# count_reads()
# count_access_lock_ops()

print("Time: ", time.time() - start_time)