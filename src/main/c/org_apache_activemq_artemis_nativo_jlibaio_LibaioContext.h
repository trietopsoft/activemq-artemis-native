/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_apache_activemq_artemis_nativo_jlibaio_LibaioContext */

#ifndef _Included_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
#define _Included_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
#ifdef __cplusplus
extern "C" {
#endif
#undef org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_EXPECTED_NATIVE_VERSION
#define org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_EXPECTED_NATIVE_VERSION 11L
/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    shutdownHook
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_shutdownHook
  (JNIEnv *, jclass);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    setForceSyscall
 * Signature: (Z)V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_setForceSyscall
  (JNIEnv *, jclass, jboolean);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    isForceSyscall
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_isForceSyscall
  (JNIEnv *, jclass);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    strError
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_strError
  (JNIEnv *, jclass, jlong);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    dumbFD
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_dumbFD
  (JNIEnv *, jclass);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    memoryAddress0
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_memoryAddress0
  (JNIEnv *, jclass, jobject);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    newContext
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_newContext
  (JNIEnv *, jobject, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    deleteContext
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_deleteContext
  (JNIEnv *, jclass, jobject);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    open
 * Signature: (Ljava/lang/String;Z)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_open
  (JNIEnv *, jclass, jstring, jboolean);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    close
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_close
  (JNIEnv *, jclass, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    newAlignedBuffer
 * Signature: (II)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_newAlignedBuffer
  (JNIEnv *, jclass, jint, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    freeBuffer
 * Signature: (Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_freeBuffer
  (JNIEnv *, jclass, jobject);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    submitWrite
 * Signature: (IJJJIJJ)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_submitWrite
  (JNIEnv *, jclass, jint, jlong, jlong, jlong, jint, jlong, jlong);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    submitRead
 * Signature: (IJJJIJJ)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_submitRead
  (JNIEnv *, jclass, jint, jlong, jlong, jlong, jint, jlong, jlong);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    submitFDataSync
 * Signature: (IJJJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_submitFDataSync
  (JNIEnv *, jclass, jint, jlong, jlong, jlong);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    poll
 * Signature: (JJII)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_poll
  (JNIEnv *, jclass, jlong, jlong, jint, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    getNativeVersion
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_getNativeVersion
  (JNIEnv *, jclass);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    lock
 * Signature: (I)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_lock
  (JNIEnv *, jclass, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    memsetBuffer
 * Signature: (Ljava/nio/ByteBuffer;I)V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_memsetBuffer
  (JNIEnv *, jclass, jobject, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    getSize
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_getSize
  (JNIEnv *, jclass, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    getBlockSizeFD
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_getBlockSizeFD
  (JNIEnv *, jclass, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    getIOContextAddress
 * Signature: (Ljava/nio/ByteBuffer;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_getIOContextAddress
  (JNIEnv *, jclass, jobject);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    getBlockSize
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_getBlockSize
  (JNIEnv *, jclass, jstring);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    fallocate
 * Signature: (IJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_fallocate
  (JNIEnv *, jclass, jint, jlong);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    fill
 * Signature: (IIJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_fill
  (JNIEnv *, jclass, jint, jint, jlong);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    fsync
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_fsync
  (JNIEnv *, jclass, jint);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    ftruncate
 * Signature: (IJ)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_ftruncate
  (JNIEnv *, jclass, jint, jlong);

/*
 * Class:     org_apache_activemq_artemis_nativo_jlibaio_LibaioContext
 * Method:    fdatasync
 * Signature: (I)I
 */
JNIEXPORT jint JNICALL Java_org_apache_activemq_artemis_nativo_jlibaio_LibaioContext_fdatasync
  (JNIEnv *, jclass, jint);

#ifdef __cplusplus
}
#endif
#endif
