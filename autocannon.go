package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/briandowns/spinner"
	"github.com/dustin/go-humanize"
	"github.com/glentiki/hdrhistogram"
	"github.com/jbenet/goprocess"
	"github.com/olekukonko/tablewriter"
	"github.com/ttacon/chalk"
	"github.com/valyala/fasthttp"
)

type resp struct {
	status  int
	latency int64
	size    int
}

func main() {
	uri := flag.String("uri", "", "The uri to benchmark against. (Required)")
	clients := flag.Int("connections", 10, "The number of connections to open to the server.")
	pipeliningFactor := flag.Int("pipelining", 1, "The number of pipelined requests to use.")
	runtime := flag.Int("duration", 10, "The number of seconds to run the autocannnon.")
	timeout := flag.Int("timeout", 10, "The number of seconds before timing out on a request.")
	debug := flag.Bool("debug", false, "A utility debug flag.")
	flag.Parse()

	if *uri == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Println(fmt.Sprintf("running %vs test @ %v", *runtime, *uri))
	fmt.Println(fmt.Sprintf("%v connections with %v pipelining factor.", *clients, *pipeliningFactor))

	proc := goprocess.Background()

	respChan, errChan, respChanWrite, errChanWrite := runClients(proc, *clients, *pipeliningFactor, time.Second*time.Duration(*timeout), *uri)

	// define stats params
	// for GET
	latencies := hdrhistogram.New(1, 10000, 5)
	requests := hdrhistogram.New(1, 1000000, 5)
	throughput := hdrhistogram.New(1, 100000000000, 5)

	var bytes int64 = 0
	var totalBytes int64 = 0
	var respCounter int64 = 0
	var totalResp int64 = 0

	resp2xx := 0
	respN2xx := 0

	errors := 0
	timeouts := 0

	// for POST
	latenciesPost := hdrhistogram.New(1, 10000, 5)
	requestsPost := hdrhistogram.New(1, 1000000, 5)
	throughputPost := hdrhistogram.New(1, 100000000000, 5)

	var bytesPost int64 = 0
	var totalBytesPost int64 = 0
	var respCounterPost int64 = 0
	var totalRespPost int64 = 0

	resp2xxPost := 0
	respN2xxPost := 0

	errorsPost := 0
	timeoutsPost := 0
	// end define stats params

	ticker := time.NewTicker(time.Second)
	runTimeout := time.NewTimer(time.Second * time.Duration(*runtime))

	spin := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	spin.Suffix = " Running Autocannon..."
	spin.Start()

	for {
		select {
		case err := <-errChan:
			errors++
			if *debug {
				fmt.Printf("there was an error for Get: %s\n", err.Error())
			}
			if err == fasthttp.ErrTimeout {
				timeouts++
			}
		case err := <-errChanWrite:
			errorsPost++
			if *debug {
				fmt.Printf("there was an error for Post: %s\n", err.Error())
			}
			if err == fasthttp.ErrTimeout {
				timeoutsPost++
			}
		case res := <-respChan:
			s := int64(res.size)
			bytes += s
			totalBytes += s
			respCounter++

			totalResp++
			if res.status >= 200 && res.status < 300 {
				latencies.RecordValue(int64(res.latency))
				resp2xx++
			} else {
				respN2xx++
			}
		case res := <-respChanWrite:
			s := int64(res.size)
			bytesPost += s
			totalBytesPost += s
			respCounterPost++

			totalRespPost++
			if res.status >= 200 && res.status < 300 {
				latenciesPost.RecordValue(int64(res.latency))
				resp2xxPost++
			} else {
				respN2xxPost++
			}

		case <-ticker.C:
			requests.RecordValue(respCounter)
			respCounter = 0
			throughput.RecordValue(bytes)
			bytes = 0

			requestsPost.RecordValue(respCounterPost)
			respCounterPost = 0
			throughputPost.RecordValue(bytesPost)
			bytesPost = 0
			// fmt.Println("done ticking")
		case <-runTimeout.C:
			spin.Stop()

			// read
			fmt.Println("")
			fmt.Println("---------- Read/GET ----------")
			fmt.Println("")
			shortLatency := tablewriter.NewWriter(os.Stdout)
			shortLatency.SetRowSeparator("-")
			shortLatency.SetHeader([]string{
				"Stat",
				"2.5%",
				"50%",
				"97.5%",
				"99%",
				"Avg",
				"Stdev",
				"Max",
			})
			shortLatency.SetHeaderColor(tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor})
			shortLatency.Append([]string{
				chalk.Bold.TextStyle("Latency"),
				fmt.Sprintf("%v ms", latencies.ValueAtPercentile(2.5)),
				fmt.Sprintf("%v ms", latencies.ValueAtPercentile(50)),
				fmt.Sprintf("%v ms", latencies.ValueAtPercentile(97.5)),
				fmt.Sprintf("%v ms", latencies.ValueAtPercentile(99)),
				fmt.Sprintf("%.2f ms", latencies.Mean()),
				fmt.Sprintf("%.2f ms", latencies.StdDev()),
				fmt.Sprintf("%v ms", latencies.Max()),
			})
			shortLatency.Render()
			fmt.Println("")

			requestsTable := tablewriter.NewWriter(os.Stdout)
			requestsTable.SetRowSeparator("-")
			requestsTable.SetHeader([]string{
				"Stat",
				"1%",
				"2.5%",
				"50%",
				"97.5%",
				"Avg",
				"Stdev",
				"Min",
			})
			requestsTable.SetHeaderColor(tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor})
			requestsTable.Append([]string{
				chalk.Bold.TextStyle("Req/Sec"),
				fmt.Sprintf("%v", requests.ValueAtPercentile(1)),
				fmt.Sprintf("%v", requests.ValueAtPercentile(2.5)),
				fmt.Sprintf("%v", requests.ValueAtPercentile(50)),
				fmt.Sprintf("%v", requests.ValueAtPercentile(97.5)),
				fmt.Sprintf("%.2f", requests.Mean()),
				fmt.Sprintf("%.2f", requests.StdDev()),
				fmt.Sprintf("%v", requests.Min()),
			})
			requestsTable.Append([]string{
				chalk.Bold.TextStyle("Bytes/Sec"),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughput.ValueAtPercentile(1)))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughput.ValueAtPercentile(2.5)))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughput.ValueAtPercentile(50)))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughput.ValueAtPercentile(97.5)))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughput.Mean()))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughput.StdDev()))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughput.Min()))),
			})
			requestsTable.Render()

			fmt.Println("")
			fmt.Println("Req/Bytes counts sampled once per second.")
			fmt.Println("")
			fmt.Println(fmt.Sprintf("%v 2xx responses, %v non 2xx responses.", resp2xx, respN2xx))
			fmt.Println(fmt.Sprintf("%v total requests in %v seconds, %s read for GET.", formatBigNum(float64(totalResp)), *runtime, humanize.Bytes(uint64(totalBytes))))
			if errors > 0 {
				fmt.Println(fmt.Sprintf("%v total errors (%v timeouts).", formatBigNum(float64(errors)), formatBigNum(float64(timeouts))))
			}

			// write
			fmt.Println("")
			fmt.Println("")
			fmt.Println("---------- Write/POST ----------")
			fmt.Println("")
			shortLatencyWrite := tablewriter.NewWriter(os.Stdout)
			shortLatencyWrite.SetRowSeparator("-")
			shortLatencyWrite.SetHeader([]string{
				"Stat",
				"2.5%",
				"50%",
				"97.5%",
				"99%",
				"Avg",
				"Stdev",
				"Max",
			})
			shortLatencyWrite.SetHeaderColor(tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor})
			shortLatencyWrite.Append([]string{
				chalk.Bold.TextStyle("Latency"),
				fmt.Sprintf("%v ms", latenciesPost.ValueAtPercentile(2.5)),
				fmt.Sprintf("%v ms", latenciesPost.ValueAtPercentile(50)),
				fmt.Sprintf("%v ms", latenciesPost.ValueAtPercentile(97.5)),
				fmt.Sprintf("%v ms", latenciesPost.ValueAtPercentile(99)),
				fmt.Sprintf("%.2f ms", latenciesPost.Mean()),
				fmt.Sprintf("%.2f ms", latenciesPost.StdDev()),
				fmt.Sprintf("%v ms", latenciesPost.Max()),
			})
			shortLatencyWrite.Render()
			fmt.Println("")

			requestsTableWrite := tablewriter.NewWriter(os.Stdout)
			requestsTableWrite.SetRowSeparator("-")
			requestsTableWrite.SetHeader([]string{
				"Stat",
				"1%",
				"2.5%",
				"50%",
				"97.5%",
				"Avg",
				"Stdev",
				"Min",
			})
			requestsTableWrite.SetHeaderColor(tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor},
				tablewriter.Colors{tablewriter.Bold, tablewriter.FgCyanColor})
			requestsTableWrite.Append([]string{
				chalk.Bold.TextStyle("Req/Sec"),
				fmt.Sprintf("%v", requestsPost.ValueAtPercentile(1)),
				fmt.Sprintf("%v", requestsPost.ValueAtPercentile(2.5)),
				fmt.Sprintf("%v", requestsPost.ValueAtPercentile(50)),
				fmt.Sprintf("%v", requestsPost.ValueAtPercentile(97.5)),
				fmt.Sprintf("%.2f", requestsPost.Mean()),
				fmt.Sprintf("%.2f", requestsPost.StdDev()),
				fmt.Sprintf("%v", requestsPost.Min()),
			})
			requestsTableWrite.Append([]string{
				chalk.Bold.TextStyle("Bytes/Sec"),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughputPost.ValueAtPercentile(1)))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughputPost.ValueAtPercentile(2.5)))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughputPost.ValueAtPercentile(50)))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughputPost.ValueAtPercentile(97.5)))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughputPost.Mean()))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughputPost.StdDev()))),
				fmt.Sprintf("%v", humanize.Bytes(uint64(throughputPost.Min()))),
			})
			requestsTableWrite.Render()

			fmt.Println("")
			fmt.Println("Req/Bytes counts sampled once per second.")
			fmt.Println("")
			fmt.Println(fmt.Sprintf("%v 2xx responses, %v non 2xx responses.", resp2xxPost, respN2xxPost))
			fmt.Println(fmt.Sprintf("%v total requests in %v seconds, %s read for POST.", formatBigNum(float64(totalRespPost)), *runtime, humanize.Bytes(uint64(totalBytesPost))))
			if errors > 0 {
				fmt.Println(fmt.Sprintf("%v total errors (%v timeouts).", formatBigNum(float64(errorsPost)), formatBigNum(float64(timeoutsPost))))
			}

			fmt.Println("Done!")

			os.Exit(0)
		}
	}
}

func formatBigNum(i float64) string {
	if i < 1000 {
		return fmt.Sprintf("%.0f", i)
	}
	return fmt.Sprintf("%.0fk", math.Round(i/1000))
}

const ranKeyRange int = 1000000
const postPercentRatio int = 4 // Meaning 1 POST approximately meaning there is 4 GET; 80/20 ratio between reads and writes

func runClients(ctx goprocess.Process, clients int, pipeliningFactor int, timeout time.Duration, uriIncoming string) (<-chan *resp, <-chan error, <-chan *resp, <-chan error) {
	// Find out how many goroutines will run in parallel
	numThreads := clients * pipeliningFactor
	// Number of random ints to generate
	var numIntsToGenerate = 100000000 // Should be enough for a few seconds of tests. 100000000 * 64bit = 800MB
	// Number of ints to be generated by each spawned goroutine thread
	var numIntsPerThread = numIntsToGenerate / numThreads
	// Channel for communicating from goroutines back to main function
	ch := make(chan []int, 1000)
	for i := 0; i < clients; i++ {
		for j := 0; j < pipeliningFactor; j++ {
			go makeRandomNumbers(numIntsPerThread, ch, ranKeyRange)
		}
	}
	intSlice := make([][][]int, clients)
	for i := 0; i < clients; i++ {
		intSlice[i] = make([][]int, pipeliningFactor)
		for j := 0; j < pipeliningFactor; j++ {
			intSlice[i][j] = <-ch
		}
	}
	fmt.Println("Generated ", numIntsToGenerate, " random int, with range [0, ", ranKeyRange, ").")

	respChan := make(chan *resp, 2*clients*pipeliningFactor)
	errChan := make(chan error, 2*clients*pipeliningFactor)
	respChanWrite := make(chan *resp, 2*clients*pipeliningFactor)
	errChanWrite := make(chan error, 2*clients*pipeliningFactor)

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	uri := fmt.Sprintf("%s/%d", uriIncoming, r1.Intn(ranKeyRange))
	u, _ := url.Parse(uri)

	for i := 0; i < clients; i++ {
		c := fasthttp.PipelineClient{
			Addr:               fmt.Sprintf("%v:%v", u.Hostname(), u.Port()),
			IsTLS:              u.Scheme == "https",
			MaxPendingRequests: pipeliningFactor,
		}

		for j := 0; j < pipeliningFactor; j++ {
			go func(i_ int, j_ int) {
				randInt := intSlice[i_][j_]
				randIndIndex := 0
				for {

					reqGet := fasthttp.AcquireRequest()
					reqGet.SetBody([]byte(""))
					uri = fmt.Sprintf("%s/%d", uriIncoming, randInt[randIndIndex])
					randIndIndex++
					reqGet.SetRequestURI(uri)

					resGet := fasthttp.AcquireResponse()

					startTime := time.Now()
					if err := c.DoTimeout(reqGet, resGet, timeout); err != nil {
						errChan <- err
					} else {
						body := resGet.Body()
						size := len(body) + 2
						resGet.Header.VisitAll(func(key, value []byte) {
							size += len(key) + len(value) + 2
						})
						respChan <- &resp{
							status:  resGet.Header.StatusCode(),
							latency: time.Now().Sub(startTime).Milliseconds(),
							size:    size,
						}
						resGet.Reset()

						// --------------- Send post below: ------------------
						if randInt[randIndIndex] < ranKeyRange/postPercentRatio {
							randIndIndex++

							i, err := strconv.Atoi(string(body))
							if err != nil {
								// fmt.Println("Atoi err:", err)
								i = 0
							}
							// fmt.Println("post: ", i + 1)
							reqPost := fasthttp.AcquireRequest()
							reqPost.SetBodyString(strconv.Itoa(i + 1))
							reqPost.Header.SetMethod("POST")
							uri = fmt.Sprintf("%s/%d", uriIncoming, randInt[randIndIndex])
							randIndIndex++
							reqPost.SetRequestURI(uri)

							resPost := fasthttp.AcquireResponse()
							startTimePost := time.Now()
							if err := c.DoTimeout(reqPost, resPost, timeout); err != nil {
								errChanWrite <- err
							} else {
								bodyPost := resPost.Body()
								sizePost := len(bodyPost) + 2
								resPost.Header.VisitAll(func(key, value []byte) {
									sizePost += len(key) + len(value) + 2
								})
								respChanWrite <- &resp{
									status:  resPost.Header.StatusCode(),
									latency: time.Now().Sub(startTimePost).Milliseconds(),
									size:    sizePost,
								}
								reqPost.Reset()
							}
						}
					}
				}
			}(i, j)
		}
	}
	return respChan, errChan, respChanWrite, errChanWrite
}

func makeRandomNumbers(numInts int, ch chan []int, intRange int) {
	source := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(source)
	result := make([]int, numInts)
	for i := 0; i < numInts; i++ {
		result[i] = generator.Intn(intRange)
	}
	ch <- result
}
