package gphxpaxos


type Group struct {
	communicate *Communicate
	config      *Config
	instance    *Instance
}

func NewGroup(logstorage LogStorage, network NetWork, masterSM *MasterStateMachine,
	groupId int32, options *Options) *Group {

	group := &Group{}
	group.config = NewConfig(options, groupId)
	group.config.SetMasterSM(masterSM)
	group.communicate = NewCommunicate(group.config, options.MyNodeInfo.NodeId, network)
	group.instance = NewInstance(group.config, logstorage, group.communicate, options.UseCheckpointReplayer)

	return group
}

func (group *Group) GetInstacne() *Instance {
	return group.instance
}

func (group *Group) StartInit() error {
	err := group.config.Init()
	if err != nil {
		return err
	}

	group.AddStateMachine(group.config.GetSystemVSM())
	group.AddStateMachine(group.config.GetMasterSM())

	return group.instance.Init()

}

func (group *Group) Start() {
	group.instance.Start()
}

func (group *Group) Stop() {
	group.instance.Stop()
}

func (group *Group) GetConfig() *Config {
	return group.config
}

func (group *Group) GetInstance() *Instance {
	return group.instance
}

func (group *Group) GetCommitter() *Committer {
	return group.instance.GetCommitter()
}

func (group *Group) GetCheckpointCleaner() *Cleaner {
	return group.instance.GetCheckpointCleaner()
}


func (group *Group) GetCheckpointReplayer() *Replayer {
	return group.instance.GetCheckpointReplayer()
}

func (group *Group) AddStateMachine(sm StateMachine) {
	group.instance.AddStateMachine(sm)
}

