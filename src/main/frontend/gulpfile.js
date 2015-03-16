var gulp = require('gulp');
var del = require('del');
var fileinclude = require('gulp-file-include');
var less = require('gulp-less');
var path = require('path');
var browserify = require('browserify');
var transform = require('vinyl-transform');
var tsd = require('gulp-tsd');
var rename = require('gulp-rename');

gulp.task('ts-typings', function()
{
    tsd({
        command: 'reinstall',
        config: './tsd.json'
    })
});
gulp.task('ts-compile', ['ts-typings'], function(cb) {
    var browserified = transform(function(filename) {
        return browserify(filename, {debug:true})
                .plugin('tsify', {module: 'commonjs', target: 'ES5'})
                .bundle()
    });
    gulp.src(['src/client.ts'])
        .pipe(browserified)
        .pipe(rename({extname:'.js'}))
        .pipe(gulp.dest('build/scripts'));
})
gulp.task('less', function() {
    gulp.src('./src/styles.less')
        .pipe(less())
        .pipe(gulp.dest('./build/css'));
});

gulp.task('fonts', function() {
    return gulp.src([
        'bower_components/fontawesome/fonts/fontawesome-webfont.*',
        'bower_components/fontface-source-sans-pro/fonts/**/*.ttf',
        'bower_components/fontface-source-sans-pro/fonts/**/*.otf',
        'bower_components/fontface-source-sans-pro/fonts/**/*.woff',
        'bower_components/fontface-source-sans-pro/fonts/**/*.eot',
    ])
    .pipe(gulp.dest('build/fonts'));
});

gulp.task('images', function() {
    return gulp.src([
        'images/*.*'
    ]
    )
    .pipe(gulp.dest('build/images'));
})
gulp.task('html', function() {
    return gulp.src('src/*.html')
               .pipe(fileinclude({
                   prefix: '@@',
                   basepath: './src/htinclude'
               }))
                .pipe(gulp.dest('./build'));
});
gulp.task('build', ['ts-compile', 'html', 'less', 'fonts', 'images']);
gulp.task('default', ['clean'], function() {
    gulp.start('build');
})
gulp.task('clean', function(cb) {
    del([
    'build/**'
    ], cb)
})
