package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ghodss/yaml"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/util"
)

func main() {
	var (
		loglevel   string
		kubeconfig string
		jobFile    string
		logsDir    string
		fromWeek   int
		toWeek     int
	)

	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Kubernetes config file")
	flag.IntVar(&fromWeek, "from-week", 0, "Week number to start, e.g. 2497")
	flag.IntVar(&toWeek, "to-week", 0, "Week number to stop, e.g. 2497")
	flag.StringVar(&jobFile, "job", "", "YAML spec of job to run")
	flag.StringVar(&logsDir, "logs-dir", "logs", "directory to store logs in")

	flag.Parse()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	util.CheckFatal("Could not get cluster config", err)
	c, err := kubernetes.NewForConfig(config)
	util.CheckFatal("Could not make Kubernetes connection", err)

	// For each week, create a Job then watch it until it finishes
	for weeknum := fromWeek; weeknum <= toWeek; weeknum++ {
		// re-read template in case it was edited
		jobTemplate, err := ioutil.ReadFile(jobFile)
		util.CheckFatal("reading file", err)

		level.Info(util.Logger).Log("msg", "starting week", "week", weeknum)

		templated := bytes.Replace(jobTemplate, []byte("{{week}}"), []byte(fmt.Sprint(weeknum)), -1)
		var jobSpec batchv1.Job
		err = yaml.Unmarshal(templated, &jobSpec)
		util.CheckFatal("unmarshalling yaml", err)

		level.Info(util.Logger).Log("msg", "starting", "job", jobSpec.Name, "week", weeknum)
		_, err = c.BatchV1().Jobs(jobSpec.Namespace).Create(&jobSpec)
		util.CheckFatal("creating job", err)

		for {
			job, err := c.BatchV1().Jobs(jobSpec.Namespace).Get(jobSpec.Name, metav1.GetOptions{})
			util.CheckFatal("getting job status", err)

			if job.Status.Succeeded == 1 {
				err = getJobLogs(c, job, logsDir, weeknum)
				util.CheckFatal("getting job logs", err)
				level.Info(util.Logger).Log("msg", "succeeded", "week", weeknum)
				policy := metav1.DeletePropagationForeground
				deleteOptions := &metav1.DeleteOptions{
					PropagationPolicy: &policy,
				}
				time.Sleep(30 * time.Second)
				err := c.BatchV1().Jobs(jobSpec.Namespace).Delete(job.Name, deleteOptions)
				util.CheckFatal("deleting job", err)
				break
			} else if job.Status.Failed > 0 {
				level.Error(util.Logger).Log("msg", "failed", "job", job.Name)
				os.Exit(1)
			} else if job.Status.Active != 1 {
				level.Error(util.Logger).Log("msg", "not active", "job", job.Name)
				os.Exit(1)
			}
			time.Sleep(30 * time.Second)
		}
	}
}

func getJobLogs(c *kubernetes.Clientset, job *batchv1.Job, logsDir string, weeknum int) error {
	jobSelector := labels.Set(job.Spec.Selector.MatchLabels).String()
	podList, err := c.CoreV1().Pods(job.Namespace).List(metav1.ListOptions{LabelSelector: jobSelector})
	if err != nil {
		return errors.Wrap(err, "getting pod list")
	}
	for _, pod := range podList.Items {
		podLogOpts := corev1.PodLogOptions{}
		req := c.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &podLogOpts)
		podLogs, err := req.Stream()
		if err != nil {
			return errors.Wrap(err, "error in opening stream")
		}
		defer podLogs.Close()

		podSuffix := strings.TrimPrefix(pod.Name, job.Name)
		filename := path.Join(logsDir, fmt.Sprintf("%d%s", weeknum, podSuffix))
		f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
		if err != nil {
			return errors.Wrap(err, "opening log file")
		}

		_, err = io.Copy(f, podLogs)
		if err != nil {
			return errors.Wrap(err, "error in copy")
		}
	}
	return nil
}
