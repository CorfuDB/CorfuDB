var gulp = require('gulp');
var del = require('del');
var fileinclude = require('gulp-file-include');

gulp.task('fonts', function() {
    return gulp.src([
        'bower_components/fontawesome/fonts/fontawesome-webfont.*'
    ])
    .pipe(gulp.dest('build/fonts'));
});
gulp.task('html', function() {
    return gulp.src('src/*.html')
               .pipe(fileinclude({
                   prefix: '@@',
                   basepath: './src/htinclude'
               }))
                .pipe(gulp.dest('./build'));
});
gulp.task('build', ['html', 'fonts']);
gulp.task('default', ['clean'], function() {
    gulp.start('build');
})
gulp.task('clean', function(cb) {
    del([
    'build/**'
    ], cb)
})
