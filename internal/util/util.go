package util

func FriendlyHash(hash string) string {
	return hash[0:16] + "..." + hash[len(hash)-16:len(hash)]
}
