# coding=utf-8
import csv

def import_csv(filename):
    data = []
    with open(filename, "r") as file:
        reader = csv.reader(file, delimiter=',')
        row = list(reader)[-1]
        columns = ["mean", row[3], "p95", row[8], "mean_rate", row[12]]
        data.append(columns)
    return data

def report(f, last_row1, last_row2):
    i = 0
    decrease = False
    while i < len(last_row1):
        if float(last_row2[i + 1]) > float(last_row1[i + 1]) * 1.1:
            decrease = True
            f.write(" X \n")
            f.write("Details: timer ")
            f.write(timername + "\n")
            f.write("%20s\t %10s\t %10s\n" % ("", "master", "new branch"))
            f.write("%20s\t %10s\t %10s\n" % (last_row1[i], last_row1[i + 1], last_row2[i + 1]))
            break
        i += 2
    if not decrease:
        f.write(" √ \n")

def sequencer_report(f):
    f.write("|********************************************|\n")
    f.write("          SequencerPerformanceTest            \n")
    f.write("|********************************************|\n")

    f.write("Query")
    data1 = import_csv("./branch1/sequencer/corfu.runtime.sequencer.query.csv")
    data2 = import_csv("./branch2/sequencer/corfu.runtime.sequencer.query.csv")
    report(f, data1[-1], data2[-1])

    f.write("Token query with conflictInfo")
    data1 = import_csv("./branch1/sequencer/corfu.runtime.sequencer.multiple-next.csv")
    data2 = import_csv("./branch2/sequencer/corfu.runtime.sequencer.multiple-next.csv")
    report(f, data1[-1], data2[-1])

    f.write("\n\n")

def corfutable_report(f):
    f.write("|********************************************|\n")
    f.write("          CorfuTalePerformanceTest            \n")
    f.write("|********************************************|\n")

    f.write("1 corfutable 1 thread: \n")
    f.write("logwrite")
    data1 = import_csv("./branch1/corfutable-1-1/corfu.runtime.object.log-write.csv")
    data2 = import_csv("./branch2/corfutable-1-1/corfu.runtime.object.log-write.csv")
    report(f, data1[-1], data2[-1])

    f.write("upcall")
    data1 = import_csv("./branch1/corfutable-1-1/corfu.runtime.object.upcall.csv")
    data2 = import_csv("./branch2/corfutable-1-1/corfu.runtime.object.upcall.csv")
    report(f, data1[-1], data2[-1])

    f.write("1 corfutable 10 threads: \n")
    f.write("logwrite")
    data1 = import_csv("./branch1/corfutable-10-1/corfu.runtime.object.log-write.csv")
    data2 = import_csv("./branch2/corfutable-10-1/corfu.runtime.object.log-write.csv")
    report(f, data1[-1], data2[-1])

    f.write("upcall")
    data1 = import_csv("./branch1/corfutable-10-1/corfu.runtime.object.upcall.csv")
    data2 = import_csv("./branch2/corfutable-10-1/corfu.runtime.object.upcall.csv")
    report(f, data1[-1], data2[-1])

    f.write("10 corfutables 10 threads: \n")
    f.write("logwrite")
    data1 = import_csv("./branch1/corfutable-10-10/corfu.runtime.object.log-write.csv")
    data2 = import_csv("./branch2/corfutable-10-10/corfu.runtime.object.log-write.csv")
    report(f, data1[-1], data2[-1])

    f.write("upcall")
    data1 = import_csv("./branch1/corfutable-10-10/corfu.runtime.object.upcall.csv")
    data2 = import_csv("./branch2/corfutable-10-10/corfu.runtime.object.upcall.csv")
    report(f, data1[-1], data2[-1])

    f.write("\n\n")

def logunit_report(f):
    f.write("|********************************************|\n")
    f.write("            LogUnitPerformanceTest            \n")
    f.write("|********************************************|\n")

    f.write("LogUnit single read & write: \n")
    f.write("read")
    data1 = import_csv("./branch1/logunit-single/corfu-perflogunit-single-read.csv")
    data2 = import_csv("./branch2/logunit-single/corfu-perflogunit-single-read.csv")
    report(f, data1[-1], data2[-1])

    f.write("write")
    data1 = import_csv("./branch1/logunit-single/corfu-perflogunit-single-write.csv")
    data2 = import_csv("./branch2/logunit-single/corfu-perflogunit-single-write.csv")
    report(f, data1[-1], data2[-1])

    f.write("LogUnit range read & write: \n")
    f.write("read")
    data1 = import_csv("./branch1/logunit-range/corfu-perflogunit-single-read.csv")
    data2 = import_csv("./branch2/logunit-range/corfu-perflogunit-single-read.csv")
    report(f, data1[-1], data2[-1])

    f.write("write")
    data1 = import_csv("./branch1/logunit-range/corfu-perflogunit-single-write.csv")
    data2 = import_csv("./branch2/logunit-range/corfu-perflogunit-single-write.csv")
    report(f, data1[-1], data2[-1])

    f.write("\n\n")

def stream_report(f):
    f.write("|********************************************|\n")
    f.write("            StreamPerformanceTest             \n")
    f.write("|********************************************|\n")

    f.write("stream single consumer & producer \n")
    f.write("consumer")
    data1 = import_csv("./branch1/stream-single/corfu-perfstream-single-consumer.csv")
    data2 = import_csv("./branch2/stream-single/corfu-perfstream-single-consumer.csv")
    report(f, data1[-1], data2[-1])

    f.write("producer")
    data1 = import_csv("./branch1/stream-single/corfu-perfstream-single-producer.csv")
    data2 = import_csv("./branch2/stream-single/corfu-perfstream-single-producer.csv")
    report(f, data1[-1], data2[-1])

    f.write("stream multiple consumer & producer \n")
    f.write("consumer")
    data1 = import_csv("./branch1/stream-multiple/corfu-perfstream-single-consumer.csv")
    data2 = import_csv("./branch2/stream-multiple/corfu-perfstream-single-consumer.csv")
    report(f, data1[-1], data2[-1])

    f.write("producer")
    data1 = import_csv("./branch1/stream-multiple/corfu-perfstream-single-producer.csv")
    data2 = import_csv("./branch2/stream-multiple/corfu-perfstream-single-producer.csv")
    report(f, data1[-1], data2[-1])

if __name__== "__main__":
    f = open("report.txt", "w+")
    sequencer_report(f)
    corfutable_report(f)
    logunit_report(f)
    stream_report(f)
    f.close()
