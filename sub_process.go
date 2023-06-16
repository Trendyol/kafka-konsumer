package kafka

// Subprocesses like API, Kafka Cronsumer
type subprocess interface {
	Start()
	Stop()
}

type subprocesses []subprocess

func newSubProcesses() subprocesses {
	return subprocesses{}
}

func (processes *subprocesses) Add(sub subprocess) {
	*processes = append(*processes, sub)
}

func (processes *subprocesses) Start() {
	for i := range *processes {
		(*processes)[i].Start()
	}
}

func (processes *subprocesses) Stop() {
	for i := range *processes {
		(*processes)[i].Stop()
	}
}
