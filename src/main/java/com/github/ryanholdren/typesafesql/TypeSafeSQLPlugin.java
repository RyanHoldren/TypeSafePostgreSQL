package com.github.ryanholdren.typesafesql;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskContainer;

public class TypeSafeSQLPlugin implements Plugin<Project> {
	@Override
	public void apply(Project project) {
		final TaskContainer tasks = project.getTasks();
		final TypeSafeSQLMainTask transcodeSql = tasks.create("createJavaFilesFromSQL", TypeSafeSQLMainTask.class);
		tasks.getByName("compileJava").dependsOn(transcodeSql);
		final TypeSafeSQLTestTask transcodeTestSql = tasks.create("createTestJavaFilesFromSQL", TypeSafeSQLTestTask.class);
		tasks.getByName("compileTestJava").dependsOn(transcodeTestSql);
	}
}