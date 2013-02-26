/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.shims;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.lang.reflect.Method;

import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.net.NetUtils;

public class HCatHadoopShims23 implements HCatHadoopShims {
    private static boolean isMR1() {
        try {
            Class<?> clasz = Class.forName("org.apache.hadoop.mapred.JobTracker");
        } catch(ClassNotFoundException cnfe) {
            return false;
        }
        return true;
    }

    @Override
    public TaskID createTaskID() {
        return new TaskID("", 0, TaskType.MAP, 0);
    }

    @Override
    public TaskAttemptID createTaskAttemptID() {
        return new TaskAttemptID("", 0, TaskType.MAP, 0, 0);
    }

    @Override
    public org.apache.hadoop.mapreduce.TaskAttemptContext createTaskAttemptContext(Configuration conf,
                                                                                   org.apache.hadoop.mapreduce.TaskAttemptID taskId) {
        return new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(
					conf instanceof JobConf? new JobConf(conf) : conf, 
					taskId);
    }

    @Override
    public org.apache.hadoop.mapred.TaskAttemptContext createTaskAttemptContext(org.apache.hadoop.mapred.JobConf conf,
                                                                                org.apache.hadoop.mapred.TaskAttemptID taskId, Progressable progressable) {
        org.apache.hadoop.mapred.TaskAttemptContext newContext = null;
        try {
            java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.TaskAttemptContextImpl.class.getDeclaredConstructor(
                org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapred.TaskAttemptID.class,
                Reporter.class);
            construct.setAccessible(true);
            newContext = (org.apache.hadoop.mapred.TaskAttemptContext) construct.newInstance(
                           new JobConf(conf), taskId, (Reporter) progressable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return newContext;
    }

    @Override
    public JobContext createJobContext(Configuration conf,
                                       JobID jobId) {
        return new JobContextImpl(conf instanceof JobConf? new JobConf(conf) : conf,
                                  jobId);
    }

    @Override
    public org.apache.hadoop.mapred.JobContext createJobContext(org.apache.hadoop.mapred.JobConf conf,
                                                                org.apache.hadoop.mapreduce.JobID jobId, Progressable progressable) {
        if (isMR1()) {
            org.apache.hadoop.mapred.JobContext newContext = null;
            // The constructor of JobContextImpl is not public in MR1.
            try {
                java.lang.reflect.Constructor construct = org.apache.hadoop.mapred.JobContextImpl.class.getDeclaredConstructor(
                        org.apache.hadoop.mapred.JobConf.class, org.apache.hadoop.mapreduce.JobID.class,
                        Progressable.class);
                construct.setAccessible(true);
                newContext = (org.apache.hadoop.mapred.JobContextImpl)construct.newInstance(conf, jobId, progressable);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return newContext;
        } else {
            return new org.apache.hadoop.mapred.JobContextImpl(
                    new JobConf(conf), jobId, (org.apache.hadoop.mapred.Reporter) progressable);
        }
    }

    @Override
    public void commitJob(OutputFormat outputFormat, Job job) throws IOException {
        // Do nothing as this was fixed by MAPREDUCE-1447.
    }

    @Override
    public void abortJob(OutputFormat outputFormat, Job job) throws IOException {
        // Do nothing as this was fixed by MAPREDUCE-1447.
    }

    @Override
    public InetSocketAddress getResourceManagerAddress(Configuration conf) {
        String addr;
        if (isMR1()) {
            addr = conf.get("mapred.job.tracker", "localhost:8012");
        } else {
            addr = conf.get("yarn.resourcemanager.address", "localhost:8032");
        }

        return NetUtils.createSocketAddr(addr);
    }

    @Override
    public String getPropertyName(PropertyName name) {
        switch (name) {
        case CACHE_ARCHIVES:
            return MRJobConfig.CACHE_ARCHIVES;
        case CACHE_FILES:
            return MRJobConfig.CACHE_FILES;
        case CACHE_SYMLINK:
            return MRJobConfig.CACHE_SYMLINK;
        }

        return "";
    }

    @Override
    public boolean isFileInHDFS(FileSystem fs, Path path) throws IOException {
        // In case of viewfs we need to lookup where the actual file is to know the filesystem in use.
        // resolvePath is a sure shot way of knowing which file system the file is.
        return "hdfs".equals(fs.resolvePath(path).toUri().getScheme());
    }

    @Override
    public boolean needsTaskCommit(OutputCommitter committer, TaskAttemptContext taskAttemptContext) throws IOException {
        // For MR1 or non FileOutputCommitter promote committer value
        if(isMR1() || !(committer instanceof FileOutputCommitter)) {
          return committer.needsTaskCommit(taskAttemptContext);
        }

        // At this point we know that we have FileOutputCommitter on MR2.
        // Let's go to reflection and verify if output path equals to task attempt path
        try {
            Method methodGetOutputPath = FileOutputCommitter.class.getDeclaredMethod("getOutputPath", TaskAttemptContext.class);
            methodGetOutputPath.setAccessible(true);
            Path outputPath = (Path) methodGetOutputPath.invoke(null, taskAttemptContext);

            Method methodGetTaskAttemptPath = FileOutputCommitter.class.getDeclaredMethod("getTaskAttemptPath", TaskAttemptContext.class);
            methodGetTaskAttemptPath.setAccessible(true);
            Path taskAttemptPath = (Path) methodGetTaskAttemptPath.invoke(committer, taskAttemptContext);

            // This should not happen in FileOutputCommitter
            if(outputPath == null || taskAttemptPath == null) {
              return false;
            }

            // We don't need commit if we are directly writing into the
            // output directory.
            if(outputPath.equals(taskAttemptPath)) {
              return false;
            }

            return true;
        } catch(Exception ex) {
            return committer.needsTaskCommit(taskAttemptContext);
        }

    }

}
