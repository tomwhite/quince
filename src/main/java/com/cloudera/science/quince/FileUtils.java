/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.science.quince;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public final class FileUtils {
  private FileUtils() {
  }

  public static Path[] findVcfs(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.isDirectory(path)) {
      FileStatus[] fileStatuses = fs.listStatus(path, new HiddenPathFilter());
      Path[] vcfs = new Path[fileStatuses.length];
      int i = 0;
      for (FileStatus status : fileStatuses) {
        vcfs[i++] = status.getPath();
      }
      return vcfs;
    } else {
      return new Path[] { path };
    }
  }


  public static boolean sampleGroupExists(Path path, Configuration conf, String
      sampleGroup)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      return false;
    }
    for (FileStatus chrStatus : fs.listStatus(path, new PartitionPathFilter("chr"))) {
      for (FileStatus posStatus : fs.listStatus(chrStatus.getPath(),
          new PartitionPathFilter("pos"))) {
        if (fs.listStatus(posStatus.getPath(),
            new PartitionPathFilter("sample_group", sampleGroup)).length > 0) {
          return true;
        }
      }
    }
    return false;
  }

  public static void deleteSampleGroup(Path path, Configuration conf, String sampleGroup)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      return;
    }
    for (FileStatus chrStatus : fs.listStatus(path, new PartitionPathFilter("chr"))) {
      for (FileStatus posStatus : fs.listStatus(chrStatus.getPath(),
          new PartitionPathFilter("pos"))) {
        for (FileStatus sampleGroupStatus : fs.listStatus(posStatus.getPath(),
            new PartitionPathFilter("sample_group", sampleGroup))) {
          fs.delete(sampleGroupStatus.getPath(), true);
        }
      }
    }
  }

  static class HiddenPathFilter implements PathFilter {
    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  }

  static class PartitionPathFilter implements PathFilter {
    private final String partitionName;
    private final String partitionValue;
    public PartitionPathFilter(String partitionName) {
      this(partitionName, null);
    }
    public PartitionPathFilter(String partitionName, String partitionValue) {
      this.partitionName = partitionName;
      this.partitionValue = partitionValue;
    }
    @Override
    public boolean accept(Path path) {
      if (partitionValue == null) {
        return path.getName().startsWith(partitionName + "=");
      } else {
        return path.getName().equals(partitionName + "=" + partitionValue);
      }
    }
  }
}
