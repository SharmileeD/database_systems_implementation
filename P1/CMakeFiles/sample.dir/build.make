# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.12

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/babbi/Workspace/database_systems_implementation/P1

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/babbi/Workspace/database_systems_implementation/P1

# Include any dependencies generated for this target.
include CMakeFiles/sample.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/sample.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/sample.dir/flags.make

CMakeFiles/sample.dir/DBFile.cc.o: CMakeFiles/sample.dir/flags.make
CMakeFiles/sample.dir/DBFile.cc.o: DBFile.cc
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/babbi/Workspace/database_systems_implementation/P1/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/sample.dir/DBFile.cc.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/sample.dir/DBFile.cc.o -c /home/babbi/Workspace/database_systems_implementation/P1/DBFile.cc

CMakeFiles/sample.dir/DBFile.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/sample.dir/DBFile.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/babbi/Workspace/database_systems_implementation/P1/DBFile.cc > CMakeFiles/sample.dir/DBFile.cc.i

CMakeFiles/sample.dir/DBFile.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/sample.dir/DBFile.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/babbi/Workspace/database_systems_implementation/P1/DBFile.cc -o CMakeFiles/sample.dir/DBFile.cc.s

# Object files for target sample
sample_OBJECTS = \
"CMakeFiles/sample.dir/DBFile.cc.o"

# External object files for target sample
sample_EXTERNAL_OBJECTS =

sample: CMakeFiles/sample.dir/DBFile.cc.o
sample: CMakeFiles/sample.dir/build.make
sample: /usr/lib/x86_64-linux-gnu/libgtest.a
sample: CMakeFiles/sample.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/babbi/Workspace/database_systems_implementation/P1/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable sample"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/sample.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/sample.dir/build: sample

.PHONY : CMakeFiles/sample.dir/build

CMakeFiles/sample.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/sample.dir/cmake_clean.cmake
.PHONY : CMakeFiles/sample.dir/clean

CMakeFiles/sample.dir/depend:
	cd /home/babbi/Workspace/database_systems_implementation/P1 && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/babbi/Workspace/database_systems_implementation/P1 /home/babbi/Workspace/database_systems_implementation/P1 /home/babbi/Workspace/database_systems_implementation/P1 /home/babbi/Workspace/database_systems_implementation/P1 /home/babbi/Workspace/database_systems_implementation/P1/CMakeFiles/sample.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/sample.dir/depend

