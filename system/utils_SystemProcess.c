#include "utils_SystemProcess.h"

JNIEXPORT jint JNICALL Java_utils_SystemProcess_systemCall(JNIEnv *env, jobject obj, jstring jcmd){
    jboolean iscopy;
    const char *cmd = (*env)->GetStringUTFChars(env, jcmd, &iscopy);
    jint ec = system(cmd); 
    (*env)->ReleaseStringUTFChars(env, jcmd, cmd);
    return ec;
}
