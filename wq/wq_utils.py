def give_task_specs(task, num_cores=1, memory=3500, disk=3500):
	task.specify_cores(num_cores)
	task.specify_memory(memory)
	task.specify_disk(disk)
	task.specify_environment('just-dill.tar.gz')
	return task
