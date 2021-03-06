// +build !windows

package nsqd

// backendName = topicName + ":" + channelName
func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
	backendName := topicName + ":" + channelName
	return backendName
}
